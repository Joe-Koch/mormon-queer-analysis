from dagster import ConfigurableResource
from openai import OpenAI


class OpenAIClientResource(ConfigurableResource):
    openai_api_key: str

    def get_client(self) -> OpenAI:
        return OpenAI(api_key=self.openai_api_key)
