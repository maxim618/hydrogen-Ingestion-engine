-- KEYS[1]: Ключ счетчика для конкретной ячейки, например: "rate:pemfc-v2-01"
-- ARGV[1]: Лимит сообщений в секунду (например, "100")

local key = KEYS[1]
local limit = tonumber(ARGV[1])

-- Инкрементируем счетчик сообщений
local current = redis.call("INCR", key)

-- Если это первое сообщение за секунду, устанавливаем время жизни ключа в 1 секунду
if current == 1 then
    redis.call("EXPIRE", key, 1)
end

-- Если лимит превышен, возвращаем 0 (отсечь спам)
if current > limit then
    return 0
else
    return 1
end
