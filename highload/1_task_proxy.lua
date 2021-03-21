
yaml = require('yaml')

local f = io.open("config.yml", "rb")
local content = f:read("*all")
f:close()


parse_content = yaml.decode(content)
port = parse_content.proxy.port
host = parse_content.proxy.bypass.host
host_port = parse_content.proxy.bypass.port
http_client = require('http.client').new()

local function proxy_send(req) 
    res = http_client:request(req:method(), host .. ':' .. host_port .. req:path())
    return {
        status = res.status,
        headers = res.headers,
        body = res.body
    }
end

server = require('http.server').new('localhost', port)
router = require('http.router').new()

router:route({path = '/.*'}, proxy_send)
router:route({path = '/'}, proxy_send)
server:set_router(router)

server:start()
