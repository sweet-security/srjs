import { SchemaRegistryAPIClientOptions } from '@types'
import type { Message } from '@sweet-security/kafkas'
import { SchemaRegistryAPIClientArgs } from './api'
import SchemaRegistry from './SchemaRegistry'

export interface ParsedMessage<T> extends Omit<Message, 'value'> {
  value: T
}
export type Handler = <T>(message: ParsedMessage<T>) => void
export interface EntryToHandlers {
  [entryType: string]: {
    [entryMajorVersion: string]: {
      [entryMinorVersion: string]: Handler
    }
  }
}

export class HandlerManager {
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

  async handleMessage(message: Message): Promise<void> {
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
  ): Handler | null {
    const versions = this.entryToHandlers[entryType]
    if (!versions) return null

    if (entryMajorVersion in versions) {
      if (entryMinorVersion in versions[entryMajorVersion]) {
        return versions[entryMajorVersion][entryMinorVersion]
      }
      const latestMinor = Object.keys(versions).reduce((max, c) => (c > max ? c : max))
      return versions[entryMajorVersion][latestMinor]
    }
    return null
  }
}
