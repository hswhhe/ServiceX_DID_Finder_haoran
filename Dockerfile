FROM python:3

WORKDIR /home/did
COPY src/* .

COPY requirements.txt requirements.txt
RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt

RUN python -m pip list > /python_installed_packages.txt

# build timestamp
RUN echo "Timestamp:" `date --utc` | tee /image-build-info.txt

ENTRYPOINT [ "python3", "/home/did/demo_did.py" ]