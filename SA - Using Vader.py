#!/usr/bin/env python
# coding: utf-8

# ## SA - Using Vader
# 
# New notebook

# #### Importing Libraries

# In[52]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install vaderSentiment
# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install sentencepiece transformers
# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %pip install azure-ai-textanalytics


# In[53]:


import json
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col, explode_outer, lit
from pyspark.sql.types import StringType, ArrayType, IntegerType, BooleanType, MapType, StructType, StructField
from datetime import datetime
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# ##### Data Loading

# In[54]:


# Load pre-filtered data
df = spark.read.format("delta").load("Tables/conversationaltranscript_current_semester")

# reading the watermark
watermark_df = spark.read.format("delta").load("Tables/etl_watermark")
batch_date = watermark_df.collect()[0]["last_processed_date"]

# Initializin the logging date
start_time = datetime.now()


# ##### Data Preparation
# 
# 
# 

# In[55]:


# Parse JSON content and extract only needed fields
def safe_json_loads(val):
    try:
        return json.loads(val)
    except Exception:
        return None

def parse_conversation_enhanced(content):
    try:
        data = json.loads(content)
        activities = data.get('activities', [])
        session_info = None
        student_info = {}
        messages = []
        user_queries = []
        user_selections = []
        last_user_intent = None
        for activity in activities:
            if activity.get('type') == 'trace' and activity.get('valueType') == 'SessionInfo':
                session_info = activity.get('value', {})
            elif activity.get('type') == 'trace' and activity.get('valueType') == 'VariableAssignment':
                val = activity.get('value', {})
                name = val.get('name')
                new_value = val.get('newValue')
                if name == 'StudentInfo' and new_value:
                    student_info = safe_json_loads(new_value) or {}
            elif activity.get('type') == 'trace' and activity.get('valueType') == 'UnknownIntent':
                user_query = activity.get('value', {}).get('userQuery', '')
                if user_query:
                    user_queries.append({'query': user_query, 'timestamp': activity.get('timestamp')})
            elif activity.get('type') == 'message' and activity.get('value', {}):
                action_id = activity.get('value', {}).get('actionSubmitId')
                if action_id:
                    user_selections.append({'selection': action_id, 'timestamp': activity.get('timestamp')})
            elif activity.get('type') == 'trace' and activity.get('valueType') == 'IntentRecognition':
                last_user_intent = activity.get('value', {}).get('intentTitle')
            elif activity.get('type') == 'message':
                messages.append({
                    'role': 'user' if activity.get('from', {}).get('role') == 1 else 'bot',
                    'text': activity.get('text') or '',
                    'timestamp': activity.get('timestamp')
                })
        return {
            'start_time': session_info.get('startTimeUtc') if session_info else None,
            'end_time': session_info.get('endTimeUtc') if session_info else None,
            'outcome': session_info.get('outcome') if session_info else None,
            'outcome_reason': session_info.get('outcomeReason') if session_info else None,
            'student_country': student_info.get('StudentCountry'),
            'expected_graduation_year': student_info.get('ExpectedGraduationYear'),
            'student_degree': student_info.get('StudentDegree'),
            'has_holds': student_info.get('HasHolds', False),
            'hold_reason': student_info.get('HoldReason'),
            'active_pc_gathering': student_info.get('ActivePCGathering'),
            'active_ec_gathering': student_info.get('ActiveECGathering'),
            'student_name': student_info.get('StudentName'),
            'student_id': student_info.get('StudentNumber', '').strip(),
            'ContactGUID': student_info.get('ContactGUID', '').strip(),
            'last_intent': last_user_intent,
            'user_queries': user_queries,
            'user_selections': user_selections,
            'first_user_query': user_queries[0]['query'] if user_queries else None,
            'main_user_selection': user_selections[0]['selection'] if user_selections else None,
        }
    except Exception as e:
        print(f"Error parsing content: {str(e)}")
        return None

schema = StructType([
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("outcome", StringType(), True),
    StructField("outcome_reason", StringType(), True),
    StructField("student_country", StringType(), True),
    StructField("expected_graduation_year", StringType(), True),
    StructField("student_degree", StringType(), True),
    StructField("has_holds", BooleanType(), True),
    StructField("hold_reason", StringType(), True),
    StructField("active_pc_gathering", StringType(), True),
    StructField("active_ec_gathering", StringType(), True),
    StructField("student_name", StringType(), True),
    StructField("student_id", StringType(), True),
    StructField("ContactGUID", StringType(), True),
    StructField("last_intent", StringType(), True),
    StructField("user_queries", ArrayType(MapType(StringType(), StringType())), True),
    StructField("user_selections", ArrayType(MapType(StringType(), StringType())), True),
    StructField("first_user_query", StringType(), True),
    StructField("main_user_selection", StringType(), True),
])

parse_conversation_udf = udf(parse_conversation_enhanced, schema)

processed_df = df.withColumn(
    "analysis", 
    parse_conversation_udf(col("content"))
).select(
    "bot_conversationtranscriptid",
    "modifiedon",
    "bot_conversationtranscriptidname",
    "content",
    "analysis.*"
).filter(
    col("start_time").isNotNull()
)


# In[56]:


print(f"Total parsed: {processed_df.count()}")
display(processed_df)


# In[57]:


# Explode user_queries so each user prompt is a separate row
user_prompts_df = ( 
    processed_df
    .select(
        "bot_conversationtranscriptid",
        "student_id",
        "ContactGUID",
        "student_name",
        "modifiedon",
        "start_time",
        "end_time",
        "outcome",
        "outcome_reason",
        "main_user_selection",
        "student_country",
        "expected_graduation_year",
        "student_degree",
        "has_holds",
        "hold_reason",
        "active_pc_gathering",
        "active_ec_gathering",
        explode_outer("user_queries").alias("user_query_struct")
    )
    .select(
        "bot_conversationtranscriptid",
        "student_id",
        "ContactGUID",
        "student_name",
        "modifiedon",
        "start_time",
        "end_time",
        "outcome",
        "outcome_reason",
        "main_user_selection",
        "student_country",
        "expected_graduation_year",
        "student_degree",
        "has_holds",
        "hold_reason",
        "active_pc_gathering",
        "active_ec_gathering",
        col("user_query_struct.query").alias("user_prompt")
    )
    .filter(col("user_query_struct.query").isNotNull())
    .toPandas()
)
print(f"exploded records: {user_prompts_df.count()}")
display(user_prompts_df)


# In[14]:


# Explode user_queries so each user prompt is a separate row
user_prompts_df = processed_df.select(
    "bot_conversationtranscriptid",
    "student_id",
    "ContactGUID",
    "student_name",
    "modifiedon",
    "start_time",
    "end_time",
    "outcome",
    "outcome_reason",
    "main_user_selection",
    "student_country",
    "expected_graduation_year",
    "student_degree",
    "has_holds",
    "hold_reason",
    "active_pc_gathering",
    "active_ec_gathering",
    explode_outer("user_queries").alias("user_query_struct")
).select(
    "bot_conversationtranscriptid",
    "student_id",
    "ContactGUID",
    "student_name",
    "modifiedon",
    "start_time",
    "end_time",
    "outcome",
    "outcome_reason",
    "main_user_selection",
    "student_country",
    "expected_graduation_year",
    "student_degree",
    "has_holds",
    "hold_reason",
    "active_pc_gathering",
    "active_ec_gathering",
    col("user_query_struct.query").alias("user_prompt")
)
print(f"exploded records: {user_prompts_df.count()}")
display(user_prompts_df)


# #### Sentiment Analysis

# In[58]:


from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient

# Azure credentials and client setup
credential = AzureKeyCredential("cc9f71463aca40a6b2ee6f0a7d7a41f4")
endpoint = "https://eastus.api.cognitive.microsoft.com/"
text_analytics_client = TextAnalyticsClient(endpoint, credential)

# Function to batch documents
def batch_documents(documents, batch_size=10):
    for i in range(0, len(documents), batch_size):
        yield documents[i:i + batch_size]

# Prepare document list
all_documents = user_prompts_df["user_prompt"].tolist()

# Collect results into a list of dictionaries

sentiments = []
positive_scores = []
neutral_scores = []
negative_scores = []



for batch in batch_documents(all_documents):
    response = text_analytics_client.analyze_sentiment(batch, language="en")
    for doc in response:
        if not doc.is_error:
            sentiments.append(doc.sentiment)
            positive_scores.append(doc.confidence_scores.positive)
            neutral_scores.append(doc.confidence_scores.neutral)
            negative_scores.append(doc.confidence_scores.negative)
        else:
            sentiments.append(None)
            positive_scores.append(None)
            neutral_scores.append(None)
            negative_scores.append(None)


# Convert results to DataFrame

user_prompts_df["sentiment"] = sentiments
#user_prompts_df["positive_score"] = positive_scores
#user_prompts_df["neutral_score"] = neutral_scores
#user_prompts_df["negative_score"] = negative_scores


# In[61]:


df_result = spark.createDataFrame(user_prompts_df)

display(df_result)


# In[7]:


# Sentiment analysis using VADER, returning only the sentiment label

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

# Pandas UDF that returns only the sentiment label
@pandas_udf(StringType())
def vader_sentiment_label_udf(texts: pd.Series) -> pd.Series:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()
    labels = []
    for text in texts:
        vs = analyzer.polarity_scores(text if text else "")
        if vs["compound"] >= 0.05:
            label = "positive"
        elif vs["compound"] <= -0.05:
            label = "negative"
        else:
            label = "neutral"
        labels.append(label)
    return pd.Series(labels)

# Apply the UDF to user_prompt, keeping only the sentiment label
user_prompts_with_sentiment = user_prompts_df.withColumn(
    "sentiment",
    vader_sentiment_label_udf("user_prompt")
)

display(user_prompts_with_sentiment)
print(f"Student messages count: {user_prompts_with_sentiment.count()}")


# In[62]:


# Adding the batch date to the final table
user_prompts_with_sentiment = df_result.withColumn("batch_date", lit(batch_date))

#user_prompts_with_sentiment.write.mode("overwrite").format("delta").saveAsTable("companion_pwa_sentiment_analysis")
# Export final table to lakehouse/Delta table
Sentiment_Analisys_path = "Tables/Companion_PWA_Sentiment_Analysis"

user_prompts_with_sentiment.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"batch_date = '{batch_date}'") \
    .save(Sentiment_Analisys_path)


# In[10]:


# Loggin for the data processed
log_data = [
    {
        "batch_date": batch_date,
        "records_processed": user_prompts_with_sentiment.count(),
        "start_time": start_time,
        "end_time": datetime.now(),
        "status": "OK",
        "error_message": ""
    }
]
log_df = spark.createDataFrame(log_data)
log_df.write.format("delta").mode("append").save("Tables/sentiment_analysis_etl_logs")

