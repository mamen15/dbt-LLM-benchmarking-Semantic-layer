with open("ACME_Insurance/DDL/ACME_small.ddl", "r") as ddl_file:
    sql_ddl = ddl_file.read()
print(sql_ddl)
selected_challenges = [
            "What is the total amount of premiums that a policy holder has paid by policy number?",
            "What is the average time to settle a claim by policy number?",
            "What is the total amount of premiums that a policy holder has paid?",
            "How many policies have agents sold by agent id?",
            "What is the total loss amounts, which is the sum of loss payment, loss reserve amount by claim number?",
            "How many policies does each policy holder have by policy holder id?",
            "What is the total amount of premiums paid by policy number?",
            "How many claims have been placed by policy number?",
            "What is the average policy size which is the the total amount of premium divided by the number of policies?",
            "How many policies do we have?",
            "How many claims do we have?"
        ]

!pip install rdflib
from rdflib import Graph, Namespace, RDF, URIRef
import pandas as pd
# Load the TTL file into an RDF graph
graph = Graph()
graph.parse("benchmark_questions.ttl", format="ttl")
# Define namespaces
QANDA = Namespace("http://models.data.world/benchmarks/QandA#")
DWT = Namespace("https://templates.data.world/")
DCT = Namespace("http://purl.org/dc/terms/")
RDF_NS = RDF
# SPARQL query to retrieve records of rdf:type dwt:SqlQuery
sparql_query = """
    PREFIX QandA: <http://models.data.world/benchmarks/QandA#>
    PREFIX dct: <http://purl.org/dc/terms/>
    PREFIX dwt: <https://templates.data.world/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT ?title ?description ?queryText ?query 
    WHERE {
    ?query rdf:type dwt:SqlQuery ;
            QandA:queryText ?queryText ;
            dct:description ?description ;
            dct:title ?title ;
    }
"""
# Execute the SPARQL query
results = graph.query(sparql_query, initNs={"QandA": QANDA, "dct": DCT, "dwt": DWT, "rdf": RDF_NS})
# Create a DataFrame from the query results
all_challenges = pd.DataFrame(results, columns=['title', 'challenge_text', 'gold_query_text', 'gold_query_id'])
# Strip leading/trailing whitespace
all_challenges["challenge_text"] = all_challenges["challenge_text"].str.strip()
filtered_challenges = all_challenges[
    all_challenges["challenge_text"].isin(selected_challenges)
]
filtered_challenges

import openai
import time
from datetime import datetime
openai.api_key = OPENAI_API_KEY
OPENAI_API_HARD_LIMIT = 80000 #tokens per minute
OPENAI_API_SOFT_LIMIT = OPENAI_API_HARD_LIMIT * 0.75

rate_limit_minute = -1
rate_limit_tokens_consumed = 0

def execute_open_ai_prompt(prompt):
    now = datetime.now()
    global rate_limit_minute
    global rate_limit_tokens_consumed
    if rate_limit_minute != now.minute:
        rate_limit_minute = now.minute
        rate_limit_tokens_consumed = 0
    else:
        if rate_limit_tokens_consumed >= OPENAI_API_SOFT_LIMIT:
            # Not strictly accurate as it's a rolling 60sec window, but close enough
            wait_seconds = (60 - now.second) + 2
            print(f"Waiting {wait_seconds}s to stay inside rate limit ({rate_limit_tokens_consumed} tokens consumed already)")
            time.sleep(wait_seconds)
            rate_limit_tokens_consumed = 0

    completion = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=2048,
    )
    rate_limit_tokens_consumed += completion["usage"]["total_tokens"]
    return completion.choices[0].message["content"]