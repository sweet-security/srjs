import { SchemaRegistryAPIClientOptions } from './@types'
import type { KafkaMessage } from '@sweet-security/kafkas'
import { SchemaRegistryAPIClientArgs } from './api'
import SchemaRegistry from './SchemaRegistry'

export interface ParsedMessage<T> extends Omit<KafkaMessage, 'value'> {
  value: T
}
export type Handler<T> = (message: ParsedMessage<T>) => Promise<void>
export type EntryToHandlers = {
  [entryType: string]: {
    [entryMajorVersion: string]: {
      [entryMinorVersion: string]: Handler<any>
    }
  }
}

export class RegistryManager {
  private readonly schemaRegistry: SchemaRegistry
  private readonly entryToHandlers: EntryToHandlers

  constructor(
    handlers: EntryToHandlers,
    schemaRegistryArgs: SchemaRegistryAPIClientArgs,
    schemaRegistryOpts?: SchemaRegistryAPIClientOptions,
  ) {
    this.schemaRegistry = new SchemaRegistry(schemaRegistryArgs, schemaRegistryOpts)
    this.entryToHandlers = handlers
  }

  async encodeMessagePayload(
    entryType: string,
    entryMajorVersion: string,
    entryMinorVersion: string,
    payload: unknown,
  ): Promise<Buffer> {
    const id = await this.schemaRegistry.getLatestVersionByMetadata(entryType, {
      properties: { entryType, entryMajorVersion, entryMinorVersion },
    })
    return await this.schemaRegistry.encode(id, payload)
  }

  async handleMessage(message: KafkaMessage): Promise<void> {
    const { entryType, entryMajorVersion, entryMinorVersion } = message.headers as Record<
      string,
      string | undefined
    >
    if (!entryType || !entryMajorVersion || !entryMinorVersion || !message.value) return

    const value = await this.schemaRegistry.decode(message.value as Buffer<ArrayBufferLike>)

    const lastKnownVersion = this.getMatchingHandler(
      entryType,
      entryMajorVersion,
      entryMinorVersion,
    )
    if (!lastKnownVersion) return

    const parsedMessage = {
      ...message,
      value,
    }
    lastKnownVersion(parsedMessage)
  }

  private getMatchingHandler(
    entryType: string,
    entryMajorVersion: string,
    entryMinorVersion: string,
  ): Handler<unknown> | null {
    const versions = this.entryToHandlers[entryType]
    if (!versions) return null

    if (entryMajorVersion in versions) {
      if (entryMinorVersion in versions[entryMajorVersion]) {
        return versions[entryMajorVersion][entryMinorVersion]
      }
      const latestMinor = Object.keys(versions[entryMajorVersion]).reduce((max, c) => (c > max ? c : max))
      return versions[entryMajorVersion][latestMinor]
    }
    return null
  }
}
