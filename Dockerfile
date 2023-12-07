FROM gcr.io/distroless/static-debian11:nonroot
ENTRYPOINT ["/baton-databricks"]
COPY baton-databricks /