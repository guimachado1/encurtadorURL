import logging
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor
import requests
from flask import Flask, request
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import shortuuid

app = Flask(__name__)
tracer_provider = TracerProvider()

# Configuração do Exporter OTLP (OpenTelemetry Protocol)
span_exporter = OTLPSpanExporter(endpoint="localhost:55680")
span_processor = BatchSpanProcessor(span_exporter)
tracer_provider.add_span_processor(span_processor)

# Configuração do Trace Provider
trace.set_tracer_provider(tracer_provider)

# Configuração do Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Instrumenta as requisições HTTP
RequestsInstrumentor().instrument()

# Configuração do Banco de Dados
Base = declarative_base()


class ShortURL(Base):
    __tablename__ = 'short_urls'
    id = Column(Integer, primary_key=True)
    original_url = Column(String)
    short_url = Column(String)


engine = create_engine('sqlite:///urls.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()


@app.route('/shorten', methods=['GET'])
def shorten_url():
    url = request.args.get('url')

    with tracer_provider.get_tracer(__name__).start_as_current_span("shorten_url"):
        logger.info(f"Received URL: {url}")

        # Lógica de encurtamento de URL
        short_url = shorten_logic(url)

        logger.info(f"Shortened URL: {short_url}")

        # Salvar no banco de dados
        save_url(url, short_url)

        return short_url


def shorten_logic(url):
    # Gera um ID curto aleatório usando a biblioteca shortuuid
    short_id = shortuuid.uuid()[:8]

    # Concatena o ID curto com a URL base para criar a URL encurtada
    short_url = f"http://localhost:5000/{short_id}"

    return short_url


def save_url(original_url, short_url):
    short_url_entry = ShortURL(original_url=original_url, short_url=short_url)
    session.add(short_url_entry)
    session.commit()


if __name__ == '__main__':
    app.run()
