# dbt-project
# STAGING CONVERSATIONS - stg_intercom__conversations.sql
with raw as (
    select *
    from {{ source('intercom', 'CONVERSATIONS') }}
),

parsed as (
    select
        id::string as conversation_id,
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at,
        state,
        open,
        priority,
        type,
        assignee: id::string as assignee_id,
        try_to_number(conversation_rating:rating) as rating,
        conversation_rating:remark::string as rating_remark,
        tags,
        waiting_since::timestamp as waiting_since
    from raw
)

select * from parsed;

# STAGING CONVERSATION_PART - stg_intercom__conversation_parts.sql
with raw as (
    select *
    from {{ source('intercom', 'CONVERSATION_PARTS') }}
),

parsed as (
    select
        id::string as part_id,
        conversation_id::string,
        created_at::timestamp as message_created_at,
        type,
        part_group,
        author:id::string as author_id,
        author:type::string as author_type,
        case 
            when author:type = 'bot' then 1 else 0 end as is_bot,
        case 
            when author:type = 'admin' then 1 else 0 end as is_admin,
        case 
            when author:type = 'user' then 1 else 0 end as is_user
    from raw
)

-- remove bot messages
select *
from parsed
where is_bot = 0;



# MARTS - fact_conversations.sql

with conv as (
    select *
    from {{ ref('stg_intercom__conversations') }}
),

first_reply as (
    select
        conversation_id,
        min(message_created_at) as first_admin_reply
    from {{ ref('stg_intercom__conversation_parts') }}
    where is_admin = 1
    group by conversation_id
)

select
    c.conversation_id,
    c.created_at,
    c.updated_at,
    c.assignee_id,
    c.rating,
    fr.first_admin_reply,
    datediff('minute', c.created_at, fr.first_admin_reply) as minutes_to_first_reply
from conv c
left join first_reply fr using (conversation_id);

# MART - fact_messages.sql
select 
    part_id,                          -- identifiant unique du message (event Intercom)
    conversation_id,                  -- conversation à laquelle appartient le message
    author_id,                        -- ID de l'expéditeur (user, admin ou bot)
    author_type,                      -- role: 'user', 'admin', 'bot'
    message_created_at                -- timestamp d'envoi du message
from {{ ref('stg_intercom__conversation_parts') }};  -- source = table staging events

