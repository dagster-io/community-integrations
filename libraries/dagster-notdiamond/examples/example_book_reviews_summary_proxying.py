import time

import dagster as dg
import dagster_openai as oai


@dg.asset(kinds={"python"})
def book_review_data(context: dg.AssetExecutionContext) -> dict:
    data = {
        "title": "Cat's Cradle",
        "author": "Kurt Vonnegut",
        "genre": "Science Fiction",
        "publicationYear": 1963,
        "reviews": [
            {
                "reviewer": "John Doe",
                "rating": 4.5,
                "content": "A thought-provoking satire on science and religion. Vonnegut's wit shines through.",
            },
            {
                "reviewer": "Jane Smith",
                "rating": 5,
                "content": "An imaginative and darkly humorous exploration of humanity's follies. A must-read!",
            },
            {
                "reviewer": "Alice Johnson",
                "rating": 3.5,
                "content": "Intriguing premise but felt a bit disjointed at times. Still enjoyable.",
            },
            {
                "reviewer": "Bob Brown",
                "rating": 2,
                "content": "Struggled to connect with the characters. Lacked substance in the plot.",
            },
            {
                "reviewer": "Charlie White",
                "rating": 4,
                "content": "Clever and engaging, with a unique perspective on the human condition.",
            },
            {
                "reviewer": "Diana Green",
                "rating": 1,
                "content": "I found it confusing and pretentious. Not my cup of tea.",
            },
        ],
    }
    context.add_output_metadata(metadata={"num_reviews": len(data.get("reviews", []))})
    return data


@dg.asset(
    kinds={"openai", "notdiamond"}, automation_condition=dg.AutomationCondition.eager()
)
def book_reviews_summary(
    context: dg.AssetExecutionContext,
    openai: oai.OpenAIResource,
    book_review_data: dict,
) -> dg.MaterializeResult:
    prompt = f"""
    Given the book reviews for {book_review_data["title"]}, provide a detailed summary:

    {'|'.join([r['content'] for r in book_review_data["reviews"]])}
    """

    with openai.get_client(context) as client:
        chat_completion = client.chat.completions.create(
            model="notdiamond",
            extra_body={
              "models": ["gpt-4o", "claude-3-5-sonnet-20240620"], # pass in the LLM options you want to route between
              "tradeoff": "cost",
              "preference_id": "YOUR_PREFERENCE_ID"
            },
            messages=[{"role": "user", "content": prompt}],
        )

    summary = chat_completion.choices[0].message.content or ""

    return dg.MaterializeResult(
        metadata={
            "summary": dg.MetadataValue.md(summary),
        }
    )


defs = dg.Definitions(
    assets=[book_review_data, book_reviews_summary],
    resources={
        "openai": oai.OpenAIResource(
            api_key=dg.EnvVar("NOTDIAMOND_API_KEY"),
            base_url="https://proxy.notdiamond.ai/v1/proxy",
        ),
    },
)
