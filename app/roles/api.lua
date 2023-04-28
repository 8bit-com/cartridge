local vshard = require('vshard')
local cartridge = require('cartridge')
local errors = require('errors')
local log = require('log')

local err_vshard_router = errors.new_class("Vshard routing error")
local err_httpd = errors.new_class("httpd error")

local function http_customer_add(req)
    local customer = req:json()

    local bucket_id = vshard.router.bucket_id(customer.customer_id)
    customer.bucket_id = bucket_id

    local _, error = err_vshard_router:pcall(
        vshard.router.call,
        bucket_id,
        'write',
        'customer_add',
        {customer}
    )

    if error then
        local resp = req:render({json = {
            info = "Internal error",
            error = error
        }})
        resp.status = 500
        return resp
    end

    local resp = req:render({json = { info = "Successfully created" }})
    resp.status = 201

    return resp
end

--local function selectall()
--    local resultset = {}
--    shards, err = vshard.router.routeall()
--    if err ~= nil then
--        error(err)
--    end
--    for uid, replica in pairs(shards) do
--        local set = replica:callro('box.space.customer:select', {{}, {limit=1000}}, {timeout=5})
--        for _, item in ipairs(set) do
--            table.insert(resultset, item)
--        end
--    end
--    return resultset
--end

local function http_customer_get(req)
    local customer_id = tonumber(req:stash('customer_id'))
    local bucket_id = vshard.router.bucket_id(customer_id)
    local customer, error = err_vshard_router:pcall(
        vshard.router.call,
        bucket_id,
        'read',
        'customer_lookup',
        {customer_id}
    )

    if error then
        local resp = req:render({json = {
            info = "Internal error",
            error = error
        }})
        resp.status = 500
        return resp
    end

    if customer == nil then
        local resp = req:render({json = { info = "Customer not found" }})
        resp.status = 404
        return resp
    end

    customer.bucket_id = nil
    local resp = req:render({json = customer})
    resp.status = 200
    return resp
end

local function http_customer_pop(req)
    local customer_id = tonumber(req:stash('customer_id'))
    local bucket_id = vshard.router.bucket_id(customer_id)
    local customer_pop, error = err_vshard_router:pcall(
        vshard.router.call,
        bucket_id,
        'read',
        'customer_handler',
        {customer_id}
    )

    if error then
        local resp = req:render({json = {
            info = "Internal error",
            error = error
        }})
        resp.status = 500
        return resp
    end

    local resp = req:render({json = customer_pop})
    resp.status = 200
    return resp
end

local function init(opts)
    rawset(_G, 'vshard', vshard)

    if opts.is_master then
        box.schema.user.grant('guest',
            'read,write,execute',
            'universe',
            nil, { if_not_exists = true }
        )
    end

    local httpd = cartridge.service_get('httpd')

    if not httpd then
        return nil, err_httpd:new("not found")
    end

    -- Навешиваем функции-обработчики
    httpd:route(
        { path = '/storage/customers/create', method = 'POST', public = true },
        http_customer_add
    )
    httpd:route(
        { path = '/storage/customers/:customer_id', method = 'GET', public = true },
        http_customer_get
    )
    httpd:route(
        { path = '/storage/customers/customer_pop/:customer_id', method = 'GET', public = true },
        http_customer_pop
    )

    return true
end

return {
    role_name = 'api',
    init = init,
    dependencies = {'cartridge.roles.vshard-router'},
    http_customer_get = http_customer_get,
}
