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
        assignee:id::string as assignee_id,
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

# MART - csm_team.sql
select *
from values
('5217337','Heloise'),
('5391224','Justine'),
('5440474','Patrick'),
('5300290','Raphael')
as t(id, name);


# MARTS - mart_conversations.sql
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
    c.state,
    c.open,
    c.assignee_id,
    c.rating,
    fr.first_admin_reply,
    datediff('minute', c.created_at, fr.first_admin_reply) as minutes_to_first_reply,

    -- dimensions temporelles
    date_trunc('day', c.created_at) as day,
    date_trunc('week', c.created_at) as week,
    date_trunc('month', c.created_at) as month,
    date_trunc('year', c.created_at) as year,
    extract(dow from c.created_at) as day_of_week

from conv c
left join first_reply fr using (conversation_id);


# MART - mart_messages.sql
select 
    part_id,                          
    conversation_id,                  
    author_id,                        
    author_type,                        
    message_created_at,

    -- dimensions temporelles
    date_trunc('day', message_created_at) as day,
    date_trunc('week', message_created_at) as week,
    date_trunc('month', message_created_at) as month,
    date_trunc('year', message_created_at) as year,
    extract(dow from message_created_at) as day_of_week

from {{ ref('stg_intercom__conversation_parts') }}; 


# MART - mart_support_performance.sql
with base as (
    select *
    from {{ ref('mart_conversations') }}
    where assignee_id in (select id from {{ ref('csm_team') }})
)

select
    assignee_id,                                      -- agent
    date_trunc('week', created_at) as week,           -- semaine de la conv
    
    count(distinct conversation_id) as total_conversations,           -- volume
    avg(minutes_to_first_reply) as avg_first_reply_time_min,  -- temps moyen réponse
    avg(rating)filter (where rating is not null) as avg_csat,            -- satisfaction moyenne
    sum(case when minutes_to_first_reply <= 5 then 1 else 0 end) 
        / nullif(count(*), 0)::float as pct_first_reply_lt_5min   -- % conversations répondue < 5min
from base
group by 1,2;


# MART - mart_conversation_details.sql
with conv as (
    select * from {{ ref('mart_conversations') }}
),
message_stats as (
    select 
        conversation_id,
        count(*) as total_messages,
        sum(case when author_type = 'admin' then 1 else 0 end) as admin_messages,
        sum(case when author_type = 'user' then 1 else 0 end) as user_messages,
        datediff('minute', 
            min(message_created_at), 
            max(message_created_at)
        ) as conversation_duration_min
    from {{ ref('mart_messages') }}
    group by 1
),
team as (
    select * from {{ ref('csm_team') }}
)
select
    c.*,
    t.name as assignee_name,
    m.total_messages,
    m.admin_messages,
    m.user_messages,
    m.conversation_duration_min,
    -- Ratio d'engagement
    m.admin_messages::float / nullif(m.user_messages, 0) as admin_user_ratio
from conv c
left join team t on c.assignee_id = t.id
left join message_stats m using (conversation_id)



## Modèle de données - marts

<img width="866" height="480" alt="Image" src="https://github.com/user-attachments/assets/0a6ed24e-b247-4156-b4bc-ebd032cc57eb" />
