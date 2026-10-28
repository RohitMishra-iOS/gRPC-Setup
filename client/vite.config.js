import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'
import https from 'https'

/**
 * Custom gRPC-Web proxy middleware.
 * Vite's built-in http-proxy gives 502 on chunked HTTPS responses from Render/Cloudflare.
 * This middleware manually pipes the request via Node's https.request instead.
 */
function grpcProxyPlugin() {
  return {
    name: 'grpc-proxy',
    configureServer(server) {
      server.middlewares.use('/grpc', (req, res) => {
        // Strip the /grpc prefix to get the real gRPC path
        const targetPath = req.url || '/'

        const options = {
          hostname: 'grpc-setup.onrender.com',
          port: 443,
          path: targetPath,
          method: req.method,
          headers: {
            ...req.headers,
            host: 'grpc-setup.onrender.com',
          },
        }

        // Handle preflight
        if (req.method === 'OPTIONS') {
          res.writeHead(200, {
            'access-control-allow-origin': '*',
            'access-control-allow-methods': 'POST, OPTIONS',
            'access-control-allow-headers': 'content-type, x-grpc-web, x-user-agent, grpc-timeout, authorization',
            'access-control-expose-headers': 'grpc-status, grpc-message, trailer, te',
          })
          res.end()
          return
        }

        const proxyReq = https.request(options, (proxyRes) => {
          res.writeHead(proxyRes.statusCode, proxyRes.headers)
          proxyRes.pipe(res)
        })

        proxyReq.on('error', (err) => {
          console.error('[grpc-proxy] error:', err.message)
          if (!res.headersSent) res.writeHead(502)
          res.end(err.message)
        })

        req.pipe(proxyReq)
      })
    },
  }
}

export default defineConfig({
  plugins: [react(), grpcProxyPlugin()],
})
