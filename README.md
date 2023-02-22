# rabbitmq-dotnet-client-1297

https://github.com/rabbitmq/rabbitmq-dotnet-client/discussions/1297

## Steps

* Start RabbitMQ using the `rabbitmq.conf` and `advanced.config` files in the `rmq/` sub-directory. This is what I ran from the root of my local `rabbitmq/rabbitmq-server` clone:
    ```
    make RABBITMQ_CONFIG_FILE="$HOME/development/lukebakken/rabbitmq-dotnet-client-1297/rmq/rabbitmq.conf" \
        RABBITMQ_ADVANCED_CONFIG_FILE="$HOME/development/lukebakken/rabbitmq-dotnet-client-1297/rmq/advanced.config" \
        PLUGINS='rabbitmq_management rabbitmq_top' run-broker
    ```
* Start the consuming program:
    ```
    cd cs
    dotnet run
    ```
* Start PerfTest to publish messages:
    ```
    cd ~/development/rabbitmq/rabbitmq-perf-test
    make ARGS='--predeclared --queue gh-1297-queue --routing-key gh-1297 --flag persistent --flag mandatory --confirm 4 --rate 1 --producers 1 --consumers 0' run
    ```
* Notice that the consumer will ack some messages, and others will time out, at which point the consumer is re-started.
* Entering `CTRL-C` will stop the application correctly.
