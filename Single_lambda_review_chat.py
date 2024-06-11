

import json
import boto3
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.memory.chat_message_histories import DynamoDBChatMessageHistory
from langchain.llms import OpenAI
from langchain.chat_models import ChatOpenAI
from langchain.chains import ConversationalRetrievalChain
import os
import uuid
import weaviate
from langchain.vectorstores import Weaviate
from langchain.callbacks import get_openai_callback
from imagine.dao.utils import decrement_subscription_tokens
from imagine.dao import UsageDao
from datetime import datetime

embeddings = OpenAIEmbeddings()

weaviate_client = weaviate.Client("http://a33e1edd33f1c4ee9972adb6b183b14f-2022113610.us-east-2.elb.amazonaws.com")

def lambda_handler(event, context):
  body = json.loads(event['body'])
  job = body['job']

  #Like/Dislike Handler
  if job in ['like', 'dislike']:
      print(event)
      try:
          db = boto3.resource("dynamodb")
          like_table = 'likes'
          dislike_table = 'dislikes'
          like_db = db.Table(like_table)
          dislike_db = db.Table(dislike_table)

          username = body.get('username')
          prompt = body.get('prompt')
          response = body.get('response')
          dt = body.get('timestamp')

          data = {
              'username': username,
              'prompt': prompt,
              'response': response,
              'timestamp': dt,
              'id': str(uuid.uuid4())
          }

          if job == 'like':
              like_db.put_item(Item=data)
              res = 'like'
          elif job == 'dislike':
              dislike_db.put_item(Item=data)
              res = 'dislike'
          stcode = 200

          return {
              'statusCode': stcode,
              'body': json.dumps({'message': res})
          }

      except Exception as e:
          return {
              'statusCode': 500,
              'body': json.dumps({'message': str(e)})
          }

  #Gptchat helper
  elif(job=='chat'):
    print(event)
    job_id = json.loads(event['body'])['job_id']
    prompt = json.loads(event['body'])['prompt']
    username = json.loads(event['body'])['username']

    print("username found --", username)
    print(job_id, prompt)

    usage_dao = UsageDao()
    current_usage = usage_dao.query_current_usage(f"user_{username}")
    item = current_usage.to_dict()


    start_date = item['start_date']
    tokens_left = item['tokens']['subscription']['available']
    print("Start date - ", start_date)
    print("Tokens left - ", tokens_left)

    print("init weaviate, qa")
    print("Job" + str(uuid.UUID(job_id).hex))
    vectorstore = Weaviate(weaviate_client, "Job" + str(uuid.UUID(job_id).hex), "content", by_text = False, embedding = embeddings)
    history = DynamoDBChatMessageHistory(table_name="kbminer-chat-history", session_id=job_id)
    hist = [("User: " + history.messages[i].content, history.messages[i+1].content) for i in range(0, len(history.messages) - 1)]
    history_tup = [(turn[0], turn[1]) for turn in hist]

    qa = ConversationalRetrievalChain.from_llm(ChatOpenAI(model = "gpt-4-1106-preview", temperature=0.2), vectorstore.as_retriever(), max_tokens_limit=6000)

    with get_openai_callback() as cb:

        response = qa({"question": prompt, "chat_history": history_tup})
        answer = response['answer']

        print("updating token for user -- ", username, start_date)
    try:
        decrement_subscription_tokens(f"user_{username}", cb.total_tokens)
        history.add_user_message(prompt)
        history.add_ai_message(response['answer'])
    except ValueError:
        print("not enough tokens")
        answer = "Sorry you do not have sufficient tokens to generate response"


    current_usage = usage_dao.query_current_usage(f"user_{username}")
    tokens_left = current_usage.to_dict()['tokens']['subscription']['available'] + current_usage.to_dict()['tokens']['topup']['available']


    return {
        'statusCode': 200,
        'body': json.dumps({
            "answer": answer,
            "tokens": str(tokens_left)
        })
    }