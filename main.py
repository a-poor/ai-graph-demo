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
      "name": "run_query",
      "parameters": {
        "type": "object",
        "properties": {
          "query": {
            "type": "string",
          },
        },
      },
    },
  },
  {
    "type": "function",
    "function": {
      "name": "next_line",
      "parameters": {},
    },
  },
]

# Define the starting system prompt
line, STORY = STORY[0], STORY[1:]
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

You are using "Kuzu" as the graph database. Note that you must create the node
and relationship tables before you can create individual nodes and relationships.

You can create node tables with a query like this:

```
CREATE NODE TABLE User (name STRING, age INT64 DEFAULT 0, reg_date DATE, PRIMARY KEY (name))
```

Note that the "PRIMARY KEY" is required for each node table -- and it should be
unique -- and for all nodes in the table, the primary key must be "name".

You can create relationship tables with a query like this:

```
CREATE REL TABLE FOLLOWS(FROM User TO User, since DATE);
```

By convention, the relationship table should be named in all caps.

If you need to create a relationship between two nodes -- say for example you
hear "John Doe is friends with Jane Smith" -- you would run the following query:

```
MATCH (p1:Person), (p2:Person)
WHERE p1.name = 'John Doe' AND p2.name = 'Jane Smith'
CREATE (p1)-[:FRIENDS_WITH]->(p2);
```

If you hear "John Doe works at Main St Hardware", and you've already created
a "Person" node for "John Doe", then you would first create the "Organization" node:

```
CREATE (o:Organization {name: 'Main St Hardware'});
```

And then create the relationship, with the following query:

```
MATCH (p:Person), (o:Organization)
WHERE p.name = 'John Doe' AND c.name = 'Main St Hardware'
CREATE (p)-[:WORKS_AT]->(c);
```

You may also query the database to get information. For example, if you want to
find all of the friends of John Doe, you would run the following query:

```
MATCH (p:Person {name: 'John Doe'})-[:FRIENDS_WITH]->(f:Person);
```

Create sensible node and relationship types based on the information you hear.
For example, if you hear "John Doe is friends with Jane Smith", you should
create a `Person` node for John and Jane, and a `FRIENDS_WITH` relationship.

If you need to alter the schema, you can do so by running the following query:

```
ALTER TABLE User ADD email STRING;
```

If you need to run a query, use the `run_query` function. Once you've made all
the necessary changes based on the input (schema changes, node creation, relationship
creation), you can call the `next_line` function to move on to the next line
of the story.
"""
        ),
      },
    ],
  },
  {
    "role": "developer",
    "content": [
      {
        "type": "text",
        "text": line,
      },
    ],
  }
]

# Start running!
i = 0
while len(STORY) > 0:
    i += 1
    if i > 50:
        print(f"Stopping after 50 iterations. There are {len(STORY)} lines left.")
        break

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
        # print(f"> {i}.{j}", "::", tc.function)
        
        if tc.function.name == "next_line":
            line, STORY = STORY[0], STORY[1:]
            messages.append({
              "role": "developer",
              "content": [{
                "type": "text",
                "text": "Next Line:\n\n" + line,
              }],
            })


        elif tc.function.name == "run_query":
            # Get the query
            args = json.loads(tc.function.arguments)
            if "query" not in args:
                print("No query found. Skipping. Args:", args)
                continue
            query = args["query"]
            print(f"> {i}.{j}", "::", query)

            # Run the query
            try:
                res = conn.execute(query)
            except Exception as e:
                out = "ERROR: " + str(e)
                print("<", out)
            else:
                # Get the result
                res = res if isinstance(res, list) else [res]
                out = []
                for r in res:
                    out = []
                    while r.has_next():
                        out.append(r.get_next())
            
            # Add a message that the query was executed
            messages.append({
              "role": "developer",
              "content": [{
                "type": "text",
                "text": "Result:\n\n" + json.dumps(out, indent=2),
              }],
            })

        else:
            print("Unknown function:", tc.function)
            raise ValueError("Unknown function")
print("Done.")

