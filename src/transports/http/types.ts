import {
  JSONRPCRequest,
  JSONRPCResponse,
  JSONRPCMessage,
  RequestId,
} from '@modelcontextprotocol/sdk/types.js';

export { JSONRPCRequest, JSONRPCResponse, JSONRPCMessage, RequestId };

/**
 * Response mode enum
 */
export type ResponseMode = 'stream' | 'batch';

/**
 * Configuration interface for the HTTP Stream transport
 */
export interface HttpStreamTransportConfig {
  /**
   * Port to run the HTTP server on, defaults to 8080
   */
  port?: number;
  host?: string;

  /**
   * Endpoint path for MCP communication, defaults to "/mcp"
   */
  endpoint?: string;

  /**
   * Configure ping mechanism for connection health verification
   */
  ping?: {
    /**
     * Interval in milliseconds for sending ping requests
     * Set to 0 to disable pings
     * Default: 30000 (30 seconds)
     */
    frequency?: number;

    /**
     * Timeout in milliseconds for waiting for a ping response
     * Default: 10000 (10 seconds)
     */
    timeout?: number;
  };

  /**
   * Response mode: stream (Server-Sent Events) or batch (JSON)
   * Defaults to 'stream'
   */
  responseMode?: ResponseMode;

  /**
   * Timeout in milliseconds for batched messages
   * Only applies when responseMode is 'batch'
   */
  batchTimeout?: number;

  /**
   * Maximum message size in bytes
   */
  maxMessageSize?: number;

  /**
   * Authentication configuration
   */
  auth?: any;

  /**
   * CORS configuration
   */
  cors?: any;
}

export const DEFAULT_HTTP_STREAM_CONFIG: HttpStreamTransportConfig = {
  port: 8080,
  host: '127.0.0.1',
  endpoint: '/mcp',
  responseMode: 'stream',
  batchTimeout: 30000,
  maxMessageSize: 4 * 1024 * 1024, // 4mb
  ping: {
    frequency: 30000, // 30 seconds
    timeout: 10000, // 10 seconds
  },
};
