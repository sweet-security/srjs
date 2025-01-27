import { AvroSchema, Schema, SchemaMetadata, SchemaType } from './@types'

type CacheEntry = { type: SchemaType; schema: Schema | AvroSchema }

export default class Cache {
  registryIdBySubject: { [key: string]: number }
  schemasByRegistryId: { [key: string]: CacheEntry }
  registryIdByMetadata: { [key: string]: number }

  constructor() {
    this.registryIdBySubject = {}
    this.schemasByRegistryId = {}
    this.registryIdByMetadata = {}
  }

  getLatestRegistryId = (subject: string): number | undefined => this.registryIdBySubject[subject]

  setLatestRegistryId = (subject: string, id: number): number => {
    this.registryIdBySubject[subject] = id

    return this.registryIdBySubject[subject]
  }

  getSchema = (registryId: number): CacheEntry | undefined => this.schemasByRegistryId[registryId]

  setSchema = (registryId: number, type: SchemaType, schema: Schema): CacheEntry => {
    this.schemasByRegistryId[registryId] = { type, schema }

    return this.schemasByRegistryId[registryId]
  }

  getRegistryIdByMetadata = (subject: string, metadata: SchemaMetadata): number | undefined => {
    return this.registryIdByMetadata[this.getMetadataKey(subject, metadata)]
  }

  setRegistryIdByMetadata = (subject: string, metadata: SchemaMetadata, id: number): number => {
    const key = this.getMetadataKey(subject, metadata)
    this.registryIdByMetadata[key] = id

    return this.registryIdByMetadata[key]
  }

  clear = (): void => {
    this.registryIdBySubject = {}
    this.schemasByRegistryId = {}
  }

  private getMetadataKey = (subject: string, { properties }: SchemaMetadata): string => {
    return `${subject}_${properties.entryType}_${properties.entryMajorVersion}_${properties.entryMinorVersion}`
  }
}
