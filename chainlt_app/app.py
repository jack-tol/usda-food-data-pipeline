import chainlit as cl
import logging
from openai import AsyncOpenAI
from langchain_pinecone import PineconeEmbeddings, PineconeVectorStore
import asyncio

client = AsyncOpenAI()
embedding_model = PineconeEmbeddings(model="multilingual-e5-large")
food_data_index = PineconeVectorStore.from_existing_index("food-data", embedding_model)

def get_food_nutrients(data):
    food_data = []
    for doc in data:
        food_info = {
            "Food Name": doc.page_content,
            "Nutrients": doc.metadata
        }
        food_data.append(food_info)
    return food_data

def generate_prompt(query, food_data):
    formatted_food_data = "\n".join(
        [f"- Food Name: {food['Food Name']}\n  Nutrients:\n    " +
         "\n    ".join([f"{key}: {value}" for key, value in food['Nutrients'].items()])
         for food in food_data]
    )

    prompt_template = f"""

    Answer the user's query: {query}

    Using the food data information provided. Not all data will be required to properly answer the user's query. Only use the data for the food which they are asking about. Don't interpolate information.
    For example, if the user asks about potential allergens, answer analytically, using the information available to you to cite the source. You have data from the FDA loaded into your context, such as food data directly from food labels, ensuring accuracy. Therefore, there is no need to advise the user to "always check the back of the packaging for the most up-to-date information," as the data provided comes from reliable sources, including public and private FDA data gathering methods.

    Nutrient values are provided as concentrations per 100 grams of the edible portion of the food. If a nutrient value is listed as 0.0, it indicates that the nutrient is present in such a small quantity that it falls below the detectable limit (Limit of Quantification (LOQ)).

    Serving size information is available, but nutrient data is consistently expressed per 100 grams/mls. If the user asks for serving size data, provide the available information, but clarify that the nutrient values themselves are not based on the serving size.

    The user may input their question about a very specific branded food item, or a more general food item, such as an ingredient. In the case where they ask about a specific branded food item by name, use only the data associated with that food item. In the case where the user asks about a general food, you'll have multiple brands of that food available in your context—choose the most general version of that product in that case.

    If the information provided isn't enough to accurately answer the question, reply with "I'm sorry, I don't have enough information to accurately answer that question."

    Make sure to start the response by mentioning the item you are referring to:

    Food Data: {formatted_food_data}
        
    Important: The first message the user will provide is just the food item. You should respond with something like "Loaded ingredient and nutrient data. Please begin asking your questions."

    """
    return prompt_template

async def retrieve_food_data_from_pinecone(query):
    retrieved_food_data = food_data_index.similarity_search(query, k=10)
    return get_food_nutrients(retrieved_food_data)

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

async def handle_conversation(user_query):
    message_history = cl.user_session.get("message_history", [])
    food_data = cl.user_session.get("food_data")

    if food_data is None:
        logging.info("Retrieving food data from Pinecone based on user query.")
        food_data = await retrieve_food_data_from_pinecone(user_query)
        cl.user_session.set("food_data", food_data)
        
        if not food_data:
            prompt = "I'm sorry, I don't have enough information to accurately answer that question."
            logging.info("No relevant food data found.")
        else:
            prompt = generate_prompt(user_query, food_data)
            logging.info(f"Generated prompt: {prompt}")
    else:
        logging.info("Using previously retrieved food data for conversation.")
        prompt = generate_prompt(user_query, food_data)

    message_history.append({"role": "user", "content": user_query})
    message_history.append({"role": "system", "content": prompt})
    cl.user_session.set("message_history", message_history)
    await stream_completion(message_history)

@cl.on_message
async def handle_message(message: cl.Message):
    logging.info("User message received")
    await handle_conversation(message.content)

@cl.on_chat_start
async def handle_chat_start():
    welcome_message = """

## Welcome to the FDA Food Assistant!

---

### Platform Overview

This platform is natively connected to the FDA Food Database, which includes comprehensive details about the ingredients and nutrient profiling of all branded foods available on US shelves. Whether you're looking for information on a specific product or a general food category, we've got the data you need to make informed choices.

---

### How It Works

- **Step 1:** To begin, simply provide the name of the food item you're interested in. 
- **Step 2:** Once the food data is retrieved, you can ask more detailed questions regarding the nutrients, ingredients, or potential allergens specific to that food.
    
**Note:** Nutrient values are provided per 100 grams of the edible portion of food. If you need serving size data, we can provide that as well, with clear explanations on how the nutrient values apply.

Feel free to ask about any food or nutrient details, and I'll provide the most accurate information available!

"""
    
    msg = cl.Message(content=welcome_message)
    await msg.send()