## Run the infra

The infrastructure must be started from the docker compose provided in `shopping-cart-service`

## Running the sample code

1. Start a first node:

    ```
    ./mvnw compile exec:exec -DAPP_CONFIG=local1.conf
    ```

2. Start `shopping-cart-service` and add item to cart

3. Notice the log output in the terminal of the `shopping-analytics-service`
