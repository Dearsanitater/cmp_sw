FROM Python:3.10.10
LABEL authors="DELL"
WORKDIR /cmp
COPY . /cmp
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 8000
CMD ["daphne", "-b", "0.0.0.0", "-p", "8000", "crm.asgi:application"]
