# dbt-project
# STAGING CONVERSATIONS
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

# STAGING CONVERSATION_PART 
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



# MARTS
