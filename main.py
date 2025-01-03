import os
import json
import shutil
import kuzu
from openai import OpenAI
from openai.types.chat import ChatCompletionToolParam, ChatCompletionMessageParam
from dotenv import load_dotenv
load_dotenv()



# Connect to the local database
DB_DIR = "./demo.db"
if os.path.exists(DB_DIR):
    shutil.rmtree(DB_DIR)
db = kuzu.Database(DB_DIR)
conn = kuzu.Connection(db)

# Create the tables
NODE_CREATES = [
    "CREATE NODE TABLE IF NOT EXISTS Person(name STRING, PRIMARY KEY (name));",
    "CREATE NODE TABLE IF NOT EXISTS Pet(name STRING, PRIMARY KEY (name));",
    "CREATE NODE TABLE IF NOT EXISTS Company(name STRING, PRIMARY KEY (name));",
    "CREATE NODE TABLE IF NOT EXISTS Education(name STRING, PRIMARY KEY (name));",
    "CREATE NODE TABLE IF NOT EXISTS Location(name STRING, PRIMARY KEY (name));",
    "CREATE NODE TABLE IF NOT EXISTS Event(name STRING, PRIMARY KEY (name));",
]

# Create the relationships
REL_CREATES = [
    "CREATE REL TABLE IF NOT EXISTS WORKS_AT(FROM Person TO Company);",
    "CREATE REL TABLE IF NOT EXISTS LIVES_IN(FROM Person TO Location);",
    "CREATE REL TABLE IF NOT EXISTS OWNS(FROM Person TO Pet);",
    "CREATE REL TABLE IF NOT EXISTS FRIENDS_WITH(FROM Person TO Person);",
    "CREATE REL TABLE IF NOT EXISTS RELATED_TO(FROM Person TO Person);",
    "CREATE REL TABLE IF NOT EXISTS GRADUATED_FROM(FROM Person TO Education);",
    "CREATE REL TABLE IF NOT EXISTS ATTENDED(FROM Person TO Event);",
]

# Run the setup queries
for query in NODE_CREATES + REL_CREATES:
    conn.execute(query)

# Define the "story"
STORY = [
    "I'm Alex Chen, a software architect at DataFlow Systems in Seattle.",
    "I've been working there since 2020, where I lead the cloud infrastructure team.",
    "I live in the Ballard neighborhood with my golden retriever, Max, who I adopted three years ago.",
    "At DataFlow, I work closely with Sarah Kim, our lead data scientist.",
    "She's brilliant with machine learning models and has been with the company for five years.",
    "Sarah's younger brother, James Kim, actually works at our competitor, CloudScale Tech, as a product manager in their enterprise division.",
    "Sarah and I collaborated on Project Phoenix last year with Marcus Rodriguez, a solutions architect who joined us from Microsoft.",
    "Marcus still mentors his former intern, Emily Patel, who's now a rising star at Stripe leading their payment authentication systems.",
    "I met my best friend Jordan Taylor at the University of Washington while getting my Master's in Computer Science.",
    "Jordan now runs a successful gaming startup, PixelForge Interactive, which they founded with their spouse, Dr. Sam Taylor, who teaches game design at DigiTech Institute.",
    "Last month, I spoke at the PNW Tech Summit, where I reconnected with my old colleague Priya Sharma.",
    "She's now the CTO at GreenLeaf Solutions, an environmental technology company.",
    "She's building out their sustainability tracking platform with Chris Martinez, who previously worked on LEED certification systems at the Green Building Council.",
    "Chris's daughter, Maya Martinez, interned at DataFlow last summer and is now finishing her Computer Engineering degree at Stanford.",
    "She's part of the same robotics research lab where my cousin, Dr. David Chen, serves as faculty advisor.",
]


# Connect to OpenAI
client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
)

# Define the "tools"
tools: list[ChatCompletionToolParam] = [
    {
        "type": "function",
        "function": {
            "name": "run_queries",
            "parameters": {
                "type": "object",
                "properties": {
                    "queries": {
                        "type": "array",
                        "items": {
                            "type": "string",
                        },
                    },
                },
            },
        },
    },
]

# Define the starting system prompt
messages: list[ChatCompletionMessageParam] = [
    {
        "role": "developer",
        "content": [
            {
                "type": "text",
                "text": (
"""
Your job is to read input from the user and use that informatio to populate
a graph database with the information you hear. You have access to a function
called `run_queries` that takes a list of queries and executes them against the
database. Those queries should be in the form of cypher queries, for example,
say you hear "there is a person named John Doe", you would run the following
cypher query:

```
CREATE (p:Person {name: 'John Doe'});
```

If you need to create a relationship between two nodes -- say for example you
hear "John Doe is friends with Jane Smith" -- you would run the following query:

```
MATCH (p1:Person), (p2:Person)
WHERE p1.name = 'John Doe' AND p2.name = 'Jane Smith'
CREATE (p1)-[:FRIENDS_WITH]->(p2);
```

If you hear "John Doe works at Main St Hardware", and you've already created
a "Person" node for "John Doe", then you would first create the "Company" node:

```
CREATE (c:Company {name: 'Main St Hardware'});
```


And then create the relationship, with the following query:

```
MATCH (p:Person), (c:Company)
WHERE p.name = 'John Doe' AND c.name = 'Main St Hardware'
CREATE (p)-[:WORKS_AT]->(c);
```

Only use create statements in that format. All other query types will fail.
Do NOT create any new node or relationship types, only use the ones that are
provided in the database setup. Don't use the any metadata properties other
than `name` on either nodes or relationships -- it will cause the statement
to fail.

If there's a piece of information that doesn't fit the format above, you can
ignore it. For example, if you hear "John Doe is a software engineer", you
can ignore the "software engineer" part and just create the node for John Doe
(assuming you haven't already created it).

For reference, the following has already been run:

```
""" + "\n".join(NODE_CREATES + REL_CREATES) + """
```

Remember to use the correct node and relationship types. For example, if you're
connecting a person to a company, you should use the `Person` and `Company`
node types, and the `WORKS_AT` relationship type. If you're connecting a person
to another person, you should use the `Person` node type and either the
`FRIENDS_WITH` or `RELATED_TO` relationship types. If the relationship wasn't
already defined, you can ignore it. For example, if you hear "John Doe works
with Jane Smith" and there isn't a `WORKS_WITH` relationship type defined (since
that information can be inferred from the `WORKS_AT` relationship) it will
depend what you already know. Assuming the nodes area already created, as well
as the "WORKS_AT" relationship between John and the company, you should create
a "WORKS_AT" relationship between Jane and the company. If you can't infer the
necessary information, you can ignore it.

already have John and the company where

already know where John works, you can just create the relationship between


you can ignore the "works with" part and just create the

You MUST include the `name` property for all nodes, since that is the primary
key for each node type. It is required when creating a new node (and it should
be unique). It is also required when creating a relationship between two nodes
(as in the above example, where the `name` is used to filter the nodes).

Please only run queries -- the user won't be able to respond to you.
"""
                ),
            },
        ],
    },
]

# Start running!
for i, line in enumerate(STORY):
    # Add the line to the messages
    print("Processing line", i, "::", line)
    messages.append({
        "role": "developer",
        "content": [
            {
                "type": "text",
                "text": line,
            },
        ],
    })
    
    # Run the completion
    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        tools=tools,
        n=1,
        tool_choice="required",
    )

    # Check the completion and execute query
    msg = completion.choices[0].message
    if not msg.tool_calls:
        print("No tool calls found. Stopping. Message:")
        print(msg)
        break
    for j, tc in enumerate(msg.tool_calls):
        # if tc.function.name is not "run_queries":
        #     print("Unexpected function call. Stopping. Function call:", tc.function)
        #     break
        args = json.loads(tc.function.arguments)
        queries = args["queries"]
        for k, query in enumerate(queries):
            print(f"> {i}.{j}.{k}", "::", query)
            conn.execute(query)
        
        # Add a message that the query was executed
        messages.append({
            "role": "developer",
            "content": [
                {
                    "type": "text",
                    "text": f"Successfully executed queries: {json.dumps(queries)}",
                },
            ],
        })
print("Done.")

