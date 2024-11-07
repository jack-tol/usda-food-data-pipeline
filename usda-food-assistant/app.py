import chainlit as cl
import logging
from openai import AsyncOpenAI
from pinecone.grpc import PineconeGRPC as Pinecone

client = AsyncOpenAI()
pc = Pinecone()
index_name = "branded-food-data"
index = pc.Index(index_name)

def get_food_nutrients(data):
    food_data = []
    for doc in data:
        food_info = {
            "Food Name": doc['metadata'].get("FOOD_NAME", "Unknown Food Name"),
            "Nutrients": doc['metadata']
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
    For example, if the user asks about potential allergens, answer analytically, using the information available to you to cite the source. You have data from the USDA FoodData Central Dataset loaded into your context, which is food data directly from food labels, which ensures accuracy. Therefore, there is no need to advise the user to "always check the back of the packaging for the most up-to-date information," as the data provided comes from reliable sources, including public and private USDA data gathering methods.

    Nutrient values are provided as concentrations per 100 grams of the edible portion of the food. If a nutrient value is listed as 0.0, it indicates that the nutrient is present in such a small quantity that it falls below the detectable limit (Limit of Quantification (LOQ)). DO NOT use square brackets in your LaTeX equations.

    Serving size information is available, but nutrient data is consistently expressed per 100 grams/milliliters. If the user asks for serving size data, provide the available information, but clarify that the nutrient values themselves are not based on the serving size.

    The user may input their question about a very specific branded food item, or a more general food item, such as an ingredient. In the case where they ask about a specific branded food item by name, use only the data associated with that food item. In the case where the user asks about a general food, you'll have data for multiple brands of that food produce available in your context, therefore choose the most general version of that product in that case or average over the nutrient data to provide the average general food ingredient in that case.

    If the context provided isn't enough to accurately answer the question, reply with "I'm sorry, I don't have enough information to accurately answer that question."
    For example, if a user asks about "Original Oreos", but the data which gets retrieved is for "Original Cookies" in that case, mention that you don't have enough information
    to accurately answer the questions about Original Oreos. Would you like me to provide general nutritional information for Original Cookies instead?". But obviously adapt
    that to the food the user has actually entered.

    Make sure to start the response by mentioning the item you are referring to:

    Food Data: {formatted_food_data}
        
    Important: The first message the user will provide is just the food item. You should respond with something like "Loaded ingredient and nutrient data for [FOOD_NAME]. Heres's some basic information: [Give some basic information about the food]. For more detailed information, please begin asking your questions. And then list out some example questions in italics such as "Does this food contain any allergens?, "Give me a detailed nutrient breakdown, including micronutrients such as vitamins and minerals.", "I have 250 grams of this food, how many calories is that, and how many grams of sugar in that amount?"
    Additionally, when appropriate, make good use of markdown formatting, such as tables, headings, bolding and italics when presenting the information to the user to make the experience visually appealing. Very important, if displaying in-line equations wrap the equations in single dollar signs $ In-line LaTex Equation Here $, and for larger, more visual equations, use double dollar signs $$ Larger more visual Multi-Line LaTeX Equation Here $$. DO NOT use square brackets in your LaTeX equations.
    """
    return prompt_template

async def similarity_search(query, top_k=10):
    query_embedding = pc.inference.embed(
        model="multilingual-e5-large",
        inputs=[query],
        parameters={"input_type": "query"}
    )

    if query_embedding and 'values' in query_embedding[0]:
        results = index.query(
            vector=query_embedding[0]['values'],
            top_k=top_k,
            include_metadata=True
        )
        return results['matches'] if results['matches'] else []
    else:
        return []

async def retrieve_food_data(query):
    raw_retrieved_food_data = await similarity_search(query)
    return get_food_nutrients(raw_retrieved_food_data)

async def stream_completion(message_history):
    msg = cl.Message(content="")
    await msg.send()

    try:
        stream = await client.chat.completions.create(
            messages=message_history,
            stream=True,
            model="gpt-4o",
            temperature=0
        )
        
        response_content = ""
        async for part in stream:
            token = part.choices[0].delta.content or ""
            await msg.stream_token(token)
            response_content += token
        
        message_history.append({"role": "assistant", "content": response_content})
        await msg.send()
    except Exception as e:
        logging.error(f"Error during stream completion: {e}")

async def handle_conversation(user_query):
    if not cl.user_session:
        logging.error("Session disconnected.")
        return
    
    message_history = cl.user_session.get("message_history", [])
    food_data = cl.user_session.get("food_data")

    if food_data is None:
        logging.info("Retrieving food data from Pinecone based on user query.")
        food_data = await retrieve_food_data(user_query)
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
    if cl.user_session.get("message_history") is None:
        cl.user_session.set("message_history", [])

    welcome_message = """

## Welcome to the USDA Food Assistant!

---

### Platform Overview

This platform is natively connected to the USDA's FoodData Central (FDC) Dataset, which includes comprehensive details about the ingredient and nutrient profiling of all foods available on US shelves. Whether you're looking for information on a specific branded food item or general ingedient, we've got the data you need to make informed choices.

---

### How It Works

- **Step 1:** To begin, simply provide the name of the food item you're interested in. 
- **Step 2:** Once the food data is retrieved, you can ask more detailed questions regarding the nutrients, ingredients, or potential allergens specific to that food.
    
**Note:** Nutrient values are provided per 100 grams of the edible portion of food. If you need serving size data, we can provide that as well, with clear explanations on how the nutrient values apply.

Feel free to ask about any food or nutrient details, and we'll provide the most accurate information available!

---

"""
    msg = cl.Message(content=welcome_message)
    await msg.send()