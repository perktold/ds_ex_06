import io.dapr.client.DaprClientBuilder

class Client(id: Int) {
    private val dapr = DaprClientBuilder().build()
    fun run() {

    }
}