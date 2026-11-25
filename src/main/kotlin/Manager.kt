import io.dapr.client.DaprClientBuilder

class Manager {
    private val dapr = DaprClientBuilder().build()
    private val lock = Any()
    fun run(){

    }
}