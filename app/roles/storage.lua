-- модуль проверки аргументов в функциях
local checks = require('checks')
local log = require('log')
local cartridge = require('cartridge')

local function init_spaces()
    local customer = box.schema.space.create(
    -- имя спейса для хранения пользователей
        'customer',
    -- дополнительные параметры
        {
            -- формат хранимых кортежей
            format = {
                {'customer_id', 'unsigned'},
                {'bucket_id', 'unsigned'},
                {'name', 'string'},
            },
            -- создадим спейс, только если его не было
            if_not_exists = true,
        }
    )

    -- создадим индекс по id пользователя
    customer:create_index('customer_id', {
        parts = {'customer_id'},
        if_not_exists = true,
    })

    customer:create_index('bucket_id', {
        parts = {'bucket_id'},
        unique = false,
        if_not_exists = true,
    })

end


-- создаём функцию на получение данных из очереди
local function customer_handler(customer_id)
    return cartridge.rpc_call('myqueue', 'customer_handler')
end

local function customer_add(customer)

    -- открытие транзакции
    box.begin()

    -- вставка кортежа в спейс customer
    box.space.customer:insert({
        customer.customer_id,
        customer.bucket_id,
        customer.name
    })

    -- коммит транзакции
    box.commit()
    cartridge.rpc_call('myqueue', 'on_replace_function', {customer})

    local http_client = require('http.client')
    local json = require('json')
    local data = 'bar'
    local body = json.encode(data)
    local headers = {
        ['Content-Type'] = 'application/json'
    }
    local response = http_client.request('POST', 'http://localhost:8383/camel/mail', body, { headers = headers })
    log.info(response)
    return true
end


local function customer_lookup(customer_id)
    checks('number')

    local customer = box.space.customer:get(customer_id)
    if customer == nil then
        return nil
    end
    customer = {
        customer_id = customer.customer_id;
        name = customer.name;
    }

    return customer
end

local exported_functions = {
    customer_add = customer_add,
    customer_lookup = customer_lookup,
    customer_handler = customer_handler,
}

local function init(opts)
    if opts.is_master then
        -- вызываем функцию инициализацию спейсов
        init_spaces()

        for name in pairs(exported_functions) do
            box.schema.func.create(name, {if_not_exists = true})
            box.schema.role.grant('public', 'execute', 'function', name, {if_not_exists = true})
        end

        --box.space.customer:on_replace(on_replace_function)
    end

    for name, func in pairs(exported_functions) do
        rawset(_G, name, func)
    end

    return true
end

return {
    role_name = 'storage',
    init = init,
    dependencies = {
        'cartridge.roles.vshard-storage',
    },
}
