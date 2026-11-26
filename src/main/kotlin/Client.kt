import io.dapr.client.DaprClientBuilder
import io.dapr.client.SubscriptionListener
import io.dapr.client.domain.CloudEvent
import io.dapr.utils.TypeRef
import reactor.core.publisher.Mono
import kotlin.random.Random

class Client(private val id: Int) {
    private val dapr = DaprClientBuilder().build()
    private val preview = DaprClientBuilder().buildPreviewClient()

    private fun request(reqId: Int) =
        dapr.publishEvent("pubsub", "requests", "$reqId,$id").block()

    private fun onGrant(msg: String) {
        val reqId = msg.split(",")[0].toInt()
        Thread {
            Thread.sleep(Random.nextLong(500, 1500))          // simulate usage
            dapr.publishEvent("pubsub", "release", "$reqId").block()
            if (reqId == id) request(id + 3)                  // second request after first release
        }.start()
    }

    fun run() {
        val sub = preview.subscribeToEvents("pubsub", "grant-$id", object : SubscriptionListener<String> {
            override fun onEvent(event: CloudEvent<String>): Mono<SubscriptionListener.Status> {
                onGrant(event.data)
                return Mono.just(SubscriptionListener.Status.SUCCESS)
            }
            override fun onError(e: RuntimeException) { e.printStackTrace() }
        }, TypeRef.STRING)
        println("Client $id initialized")
        Thread.sleep(2000)   // let subscription establish
        request(id)          // request 1–3, then 4–6 after release
        sub.awaitTermination()
    }
}