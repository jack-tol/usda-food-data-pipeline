import chainlit as cl
import logging
from openai import AsyncOpenAI
from langchain_pinecone import PineconeEmbeddings, PineconeVectorStore
import asyncio

# Initialize OpenAI client and embedding model
client = AsyncOpenAI()
embedding_model = PineconeEmbeddings(model="multilingual-e5-large")

# Load the Pinecone index
food_data_index = PineconeVectorStore.from_existing_index("food-data", embedding_model)

# Function to format retrieved food data
def get_food_nutrients(data):
    food_data = []
    for doc in data:
        food_info = {
            "Food Name": doc.page_content,
            "Nutrients": doc.metadata
        }
        food_data.append(food_info)
    return food_data

# Generate the prompt using the retrieved food data
def generate_prompt(query, food_data):
    # Create a readable structure for food data
    formatted_food_data = "\n".join(
        [f"- Food Name: {food['Food Name']}\n  Nutrients:\n    " +
         "\n    ".join([f"{key}: {value}" for key, value in food['Nutrients'].items()])
         for food in food_data]
    )

    prompt_template = f"""
    Answer the user's query: {query}

    Use the food data information provided. Only use the data for the food which they are asking about. Don't interpolate information.
    Nutrient values are provided as concentrations per 100 grams of the edible portion of the food. If a nutrient value is listed as 0.0, it indicates that the nutrient is present in such a small quantity that it falls below the detectable limit (Limit of Quantification (LOQ)).

    Here is the available food data:

    {formatted_food_data}
    """
    return prompt_template

# Retrieve and format food data from Pinecone based on the query
async def retrieve_food_data_from_pinecone(query):
    retrieved_food_data = food_data_index.similarity_search(query, k=10)
    return get_food_nutrients(retrieved_food_data)

# Stream the completion response from the OpenAI model
async def stream_completion(message_history):
    msg = cl.Message(content="")
    await msg.send()

    stream = await client.chat.completions.create(
        messages=message_history,
        stream=True,
        model="gpt-4o",
        temperature=0
    )

    async for part in stream:
        if token := part.choices[0].delta.content or "":
            await msg.stream_token(token)

    message_history.append({"role": "assistant", "content": msg.content})
    await msg.update()

# Handle the conversation flow
async def handle_conversation(user_query):
    # Retrieve message history and food data from the user session
    message_history = cl.user_session.get("message_history", [])
    food_data = cl.user_session.get("food_data")

    # If food data hasn't been retrieved yet, perform the similarity search
    if food_data is None:
        logging.info("Retrieving food data from Pinecone based on user query.")
        food_data = await retrieve_food_data_from_pinecone(user_query)
        
        # Store the retrieved food data in the session
        cl.user_session.set("food_data", food_data)
        
        # Generate prompt based on retrieved food data
        if not food_data:
            prompt = "I'm sorry, I don't have enough information to accurately answer that question."
            logging.info("No relevant food data found.")
        else:
            prompt = generate_prompt(user_query, food_data)
            logging.info(f"Generated prompt: {prompt}")
        
    else:
        # Use the already retrieved food data from the session
        logging.info("Using previously retrieved food data for conversation.")
        prompt = generate_prompt(user_query, food_data)

    # Add the user's query and the system's prompt to the message history
    message_history.append({"role": "user", "content": user_query})
    message_history.append({"role": "system", "content": prompt})
    
    # Update the session with the new message history
    cl.user_session.set("message_history", message_history)
    
    # Stream the response from the model
    await stream_completion(message_history)

# Triggered when a user message is received
@cl.on_message
async def handle_message(message: cl.Message):
    logging.info("User message received")
    await handle_conversation(message.content)

