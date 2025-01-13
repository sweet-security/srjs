import { Middleware, MiddlewareParams } from 'mappersmith'
import { DEFAULT_API_CLIENT_ID } from '../../constants'

const product = '@kafkajs/confluent-schema-registry'

const userAgentMiddleware: Middleware = ({ clientId }) => {
  const headers = userAgentHeader(clientId)
  return {
    prepareRequest: (next) => {
      return next().then((req) => req.enhance({ headers }))
    },
  }
}

export const userAgentHeader = (clientId?: string | null): Record<string, string> => {
  const comment = clientId !== DEFAULT_API_CLIENT_ID ? clientId : undefined
  const userAgent = comment ? `${product} (${comment})` : product
  return {
    'User-Agent': userAgent,
  }
}

export default userAgentMiddleware
