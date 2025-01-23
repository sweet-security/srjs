import { Type } from 'avsc'
import { Response } from 'mappersmith'

import { encode, MAGIC_BYTE } from './wireEncoder'
import decode from './wireDecoder'
import { COMPATIBILITY, MAJOR_VERSION_KEY, DEFAULT_SEPERATOR } from './constants'
import API, { SchemaRegistryAPIClientArgs, SchemaRegistryAPIClient } from './api'
import Cache from './cache'
import {
  ConfluentSchemaRegistryError,
  ConfluentSchemaRegistryArgumentError,
  ConfluentSchemaRegistryCompatibilityError,
  ConfluentSchemaRegistryValidationError,
  ResponseError,
} from './errors'
import {
  Schema,
  RawAvroSchema,
  AvroSchema,
  SchemaType,
  ConfluentSchema,
  SchemaRegistryAPIClientOptions,
  AvroConfluentSchema,
  SchemaResponse,
  ProtocolOptions,
  SchemaHelper,
  SchemaReference,
  LegacyOptions,
  SchemaMetadata,
} from './@types'
import {
  helperTypeFromSchemaType,
  schemaTypeFromString,
  schemaFromConfluentSchema,
} from './schemaTypeResolver'
import axios, { AxiosInstance, AxiosResponse } from 'axios'
import { userAgentHeader } from 'api/middleware/userAgent'
import { confluentHeaders } from 'api/middleware/confluentEncoderMiddleware'

export interface RegisteredSchema {
  id: number
}

interface AvroDecodeOptions {
  readerSchema?: RawAvroSchema | AvroSchema | Schema
}
interface DecodeOptions {
  [SchemaType.AVRO]?: AvroDecodeOptions
}

interface ConfigRequest {
  alias?: string
  compatibility?: COMPATIBILITY
  compatibilityGroup?: string
  defaultMetadata?: SchemaMetadata
  overrideMetadata?: SchemaMetadata
}

const DEFAULT_OPTS = {
  compatibility: COMPATIBILITY.FORWARD,
  separator: DEFAULT_SEPERATOR,
  compatibilityGroup: MAJOR_VERSION_KEY,
}

export default class SchemaRegistry {
  private api: SchemaRegistryAPIClient
  private alternateApi: AxiosInstance
  private cacheMissRequests: { [key: number]: Promise<Response> } = {}
  private options: SchemaRegistryAPIClientOptions | undefined

  public cache: Cache

  constructor(
    { auth, clientId, host, retry, agent }: SchemaRegistryAPIClientArgs,
    options?: SchemaRegistryAPIClientOptions,
  ) {
    this.api = API({ auth, clientId, host, retry, agent })
    // mappersmith does not support multiple query parameters with the same name but the Schema Registry API requires it.
    this.alternateApi = axios.create({
      baseURL: host,
      headers: {
        ...userAgentHeader(clientId),
        ...confluentHeaders(),
      },
    })
    this.cache = new Cache()
    this.options = options
  }

  public async getLatestVersionByMetadata(subject: string, metadata: SchemaMetadata) {
    const params = new URLSearchParams()
    Object.entries(metadata).forEach(([key, value]) => {
      params.append('key', key)
      params.append('value', value)
    })
    const response: AxiosResponse<{
      subject: string
      version: number
      id: number
      metadata: SchemaMetadata
      schema: string
    }> = await this.alternateApi.get(`/subjects/${subject}/metadata`, { params })

    return response.data
  }

  public async updateSubjectConfig(subject: string, config: ConfigRequest) {
    await this.api.Subject.updateConfig({ subject: subject, body: config })
  }

  public async isCompatible(schema: ConfluentSchema, subject: string, version: number) {
    const body = { ...schema }
    const response: Response<{ is_compatible: boolean }> = await this.api.Subject.compatible({
      subject,
      version,
      body,
    })
    return response.data().is_compatible
  }

  private isConfluentSchema(
    schema: RawAvroSchema | AvroSchema | ConfluentSchema,
  ): schema is ConfluentSchema {
    return (schema as ConfluentSchema).schema != null
  }

  private getConfluentSchema(
    schema: RawAvroSchema | AvroSchema | ConfluentSchema,
  ): ConfluentSchema {
    let confluentSchema: ConfluentSchema
    // convert data from old api (for backwards compatibility)
    if (!this.isConfluentSchema(schema)) {
      // schema is instanceof RawAvroSchema or AvroSchema
      confluentSchema = {
        type: SchemaType.AVRO,
        schema: JSON.stringify(schema),
      }
    } else {
      confluentSchema = schema as ConfluentSchema
    }
    return confluentSchema
  }

  public async register(
    schema: AvroConfluentSchema,
    subject: string,
  ): Promise<RegisteredSchema> {
    const { compatibility, compatibilityGroup } = DEFAULT_OPTS

    const confluentSchema: ConfluentSchema = this.getConfluentSchema(schema)

    const helper = helperTypeFromSchemaType(confluentSchema.type)

    const options = await this.updateOptionsWithSchemaReferences(confluentSchema, this.options)
    const schemaInstance = schemaFromConfluentSchema(confluentSchema, options)
    helper.validate(schemaInstance)
    let isFirstTimeRegistration = false

    try {
      const response = await this.api.Subject.config({ subject })
      const { compatibilityLevel }: { compatibilityLevel: COMPATIBILITY } = response.data()

      if (compatibilityLevel.toUpperCase() !== compatibility) {
        throw new ConfluentSchemaRegistryCompatibilityError(
          `Compatibility does not match the configuration (${compatibility} != ${compatibilityLevel.toUpperCase()})`,
        )
      }
    } catch (error) {
      if (error instanceof ResponseError && error.status === 404) {
        isFirstTimeRegistration = true
      } else {
        throw error
      }
    }

    const response = await this.api.Subject.register({
      subject,
      body: {
        schemaType: confluentSchema.type === SchemaType.AVRO ? undefined : confluentSchema.type,
        schema: confluentSchema.schema,
        references: confluentSchema.references,
        metadata: confluentSchema.metadata,
      },
    })

    if (compatibility && isFirstTimeRegistration) {
      await this.api.Subject.updateConfig({
        subject,
        body: {
          compatibility,
          compatibilityGroup,
        },
      })
    }

    const registeredSchema: RegisteredSchema = response.data()
    this.cache.setLatestRegistryId(subject, registeredSchema.id)
    this.cache.setSchema(registeredSchema.id, confluentSchema.type, schemaInstance)

    return registeredSchema
  }

  private async updateOptionsWithSchemaReferences(
    schema: ConfluentSchema,
    options?: SchemaRegistryAPIClientOptions,
  ) {
    const helper = helperTypeFromSchemaType(schema.type)
    const referencedSchemas = await this.getReferencedSchemas(schema, helper)

    const protocolOptions = this.asProtocolOptions(options)
    return helper.updateOptionsFromSchemaReferences(referencedSchemas, protocolOptions)
  }

  private asProtocolOptions(options?: SchemaRegistryAPIClientOptions): ProtocolOptions | undefined {
    if (!(options as LegacyOptions)?.forSchemaOptions) {
      return options as ProtocolOptions | undefined
    }
    return {
      [SchemaType.AVRO]: (options as LegacyOptions)?.forSchemaOptions,
    }
  }

  private async getReferencedSchemas(
    schema: ConfluentSchema,
    helper: SchemaHelper,
  ): Promise<ConfluentSchema[]> {
    const referencesSet = new Set<string>()
    return this.getReferencedSchemasRecursive(schema, helper, referencesSet)
  }

  private async getReferencedSchemasRecursive(
    schema: ConfluentSchema,
    helper: SchemaHelper,
    referencesSet: Set<string>,
  ): Promise<ConfluentSchema[]> {
    const references = schema.references || []

    let referencedSchemas: ConfluentSchema[] = []
    for (const reference of references) {
      const schemas = await this.getReferencedSchemasFromReference(reference, helper, referencesSet)
      referencedSchemas = referencedSchemas.concat(schemas)
    }
    return referencedSchemas
  }

  async getReferencedSchemasFromReference(
    reference: SchemaReference,
    helper: SchemaHelper,
    referencesSet: Set<string>,
  ): Promise<ConfluentSchema[]> {
    const { name, subject, version } = reference
    const key = `${name}-${subject}-${version}`

    // avoid duplicates
    if (referencesSet.has(key)) {
      return []
    }
    referencesSet.add(key)

    const versionResponse = await this.api.Subject.version(reference)
    const foundSchema = versionResponse.data() as SchemaResponse

    const schema = helper.toConfluentSchema(foundSchema)
    const referencedSchemas = await this.getReferencedSchemasRecursive(
      schema,
      helper,
      referencesSet,
    )

    referencedSchemas.push(schema)
    return referencedSchemas
  }

  private async _getSchema(
    registryId: number,
  ): Promise<{ type: SchemaType; schema: Schema | AvroSchema }> {
    const cacheEntry = this.cache.getSchema(registryId)

    if (cacheEntry) {
      return cacheEntry
    }

    const response = await this.getSchemaOriginRequest(registryId)
    const foundSchema: SchemaResponse = response.data()

    const schemaType = schemaTypeFromString(foundSchema.schemaType)

    const helper = helperTypeFromSchemaType(schemaType)
    const confluentSchema = helper.toConfluentSchema(foundSchema)

    const options = await this.updateOptionsWithSchemaReferences(confluentSchema, this.options)
    const schemaInstance = schemaFromConfluentSchema(confluentSchema, options)
    return this.cache.setSchema(registryId, schemaType, schemaInstance)
  }

  public async getSchema(registryId: number): Promise<Schema | AvroSchema> {
    return await (
      await this._getSchema(registryId)
    ).schema
  }

  public async encode(registryId: number, payload: any): Promise<Buffer> {
    if (!registryId) {
      throw new ConfluentSchemaRegistryArgumentError(
        `Invalid registryId: ${JSON.stringify(registryId)}`,
      )
    }

    const { schema } = await this._getSchema(registryId)
    try {
      const serializedPayload = schema.toBuffer(payload)
      return encode(registryId, serializedPayload)
    } catch (error) {
      if (error instanceof ConfluentSchemaRegistryValidationError) throw error

      const paths = this.collectInvalidPaths(schema, payload)
      throw new ConfluentSchemaRegistryValidationError(error, paths)
    }
  }

  private collectInvalidPaths(schema: Schema, jsonPayload: object) {
    const paths: string[][] = []
    schema.isValid(jsonPayload, {
      errorHook: (path) => paths.push(path),
    })

    return paths
  }

  public async decode(buffer: Buffer, options?: DecodeOptions): Promise<any> {
    if (!Buffer.isBuffer(buffer)) {
      throw new ConfluentSchemaRegistryArgumentError('Invalid buffer')
    }

    const { magicByte, registryId, payload } = decode(buffer)
    if (Buffer.compare(MAGIC_BYTE, magicByte) !== 0) {
      throw new ConfluentSchemaRegistryArgumentError(
        `Message encoded with magic byte ${JSON.stringify(magicByte)}, expected ${JSON.stringify(
          MAGIC_BYTE,
        )}`,
      )
    }

    const { type, schema: writerSchema } = await this._getSchema(registryId)

    let rawReaderSchema
    switch (type) {
      case SchemaType.AVRO:
        rawReaderSchema = options?.[SchemaType.AVRO]?.readerSchema as RawAvroSchema | AvroSchema
    }
    if (rawReaderSchema) {
      const readerSchema = schemaFromConfluentSchema(
        { type: SchemaType.AVRO, schema: rawReaderSchema },
        this.options,
      ) as AvroSchema
      if (readerSchema.equals(writerSchema as Type)) {
        /* Even when schemas are considered equal by `avsc`,
         * they still aren't interchangeable:
         * provided `readerSchema` may have different `opts` (e.g. logicalTypes / unionWrap flags)
         * see https://github.com/mtth/avsc/issues/362 */
        return readerSchema.fromBuffer(payload)
      } else {
        // decode using a resolver from writer type into reader type
        return readerSchema.fromBuffer(payload, readerSchema.createResolver(writerSchema as Type))
      }
    }

    return writerSchema.fromBuffer(payload)
  }

  public async getRegistryId(subject: string, version: number | string): Promise<number> {
    const response = await this.api.Subject.version({ subject, version })
    const { id }: { id: number } = response.data()

    return id
  }

  public async getRegistryIdBySchema(
    subject: string,
    schema: AvroSchema | RawAvroSchema | ConfluentSchema,
  ): Promise<number> {
    try {
      const confluentSchema: ConfluentSchema = this.getConfluentSchema(schema)
      const response = await this.api.Subject.registered({
        subject,
        body: {
          schemaType: confluentSchema.type === SchemaType.AVRO ? undefined : confluentSchema.type,
          schema: confluentSchema.schema,
        },
      })
      const { id }: { id: number } = response.data()

      return id
    } catch (error) {
      if (error instanceof ResponseError && error.status && error.status === 404) {
        throw new ConfluentSchemaRegistryError(error)
      }

      throw error
    }
  }

  public async getLatestSchemaId(subject: string): Promise<number> {
    const response = await this.api.Subject.latestVersion({ subject })
    const { id }: { id: number } = response.data()

    return id
  }

  private getSchemaOriginRequest(registryId: number) {
    // ensure that cache-misses result in a single origin request
    if (!!this.cacheMissRequests[registryId]) {
      return this.cacheMissRequests[registryId]
    } else {
      const request = this.api.Schema.find({ id: registryId }).finally(() => {
        delete this.cacheMissRequests[registryId]
      })

      this.cacheMissRequests[registryId] = request

      return request
    }
  }
}
