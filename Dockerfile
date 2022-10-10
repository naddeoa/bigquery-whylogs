from apache/beam_python3.8_sdk

ENV WHYLOGS_NO_ANALYTICS 'True'

COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install -r /tmp/requirements.txt
