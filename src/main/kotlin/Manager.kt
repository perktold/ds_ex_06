import io.dapr.client.DaprClientBuilder
import io.dapr.client.SubscriptionListener
import io.dapr.client.domain.CloudEvent
import io.dapr.utils.TypeRef
import reactor.core.publisher.Mono

enum class ResStatus { AVAILABLE, RESERVED }
enum class ReqStatus { PENDING, GRANTED, COMPLETED }

class Req(val id: Int, val client: Int, var status: ReqStatus = ReqStatus.PENDING, var res: Int = 0)

class Manager {
    private val dapr = DaprClientBuilder().build()
    private val preview = DaprClientBuilder().buildPreviewClient()
    private val lock = Any()
    private val resources = mutableMapOf(1 to ResStatus.AVAILABLE, 2 to ResStatus.AVAILABLE)
    private val requests = linkedMapOf<Int, Req>()

    private fun save(key: String, v: String) = dapr.saveState("statestore", key, v).block()

    private fun grantPending() {
        for (r in requests.values.filter { it.status == ReqStatus.PENDING }) {
            val res = resources.entries.firstOrNull { it.value == ResStatus.AVAILABLE }?.key ?: return
            resources[res] = ResStatus.RESERVED
            r.status = ReqStatus.GRANTED
            r.res = res
            save("res$res", ResStatus.RESERVED.name)
            save("req${r.id}", r.status.name)
            println("Resource $res granted to Client ${r.client}")
            dapr.publishEvent("pubsub", "grant-${r.client}", "${r.id},$res").block()
        }
    }

    private fun onRequest(msg: String) = synchronized(lock) {
        val (id, client) = msg.split(",").map { it.toInt() }
        val r = Req(id, client)
        requests[id] = r
        save("req$id", r.status.name)
        grantPending()
        if (r.status == ReqStatus.PENDING) println("Request $id pending")
    }

    private fun onRelease(msg: String) = synchronized(lock) {
        val r = requests.getValue(msg.toInt())
        r.status = ReqStatus.COMPLETED
        resources[r.res] = ResStatus.AVAILABLE
        save("req${r.id}", r.status.name)
        save("res${r.res}", ResStatus.AVAILABLE.name)
        println("Client ${r.client} released Resource ${r.res}")
        grantPending()
        if (requests.size == 6 && requests.values.all { it.status == ReqStatus.COMPLETED })
            println("All resource requests completed")
    }

    private fun listener(handler: (String) -> Unit) = object : SubscriptionListener<String> {
        override fun onEvent(event: CloudEvent<String>): Mono<SubscriptionListener.Status> {
            handler(event.data)
            return Mono.just(SubscriptionListener.Status.SUCCESS)
        }
        override fun onError(e: RuntimeException) = e.printStackTrace()
    }

    fun run() {
        resources.forEach { (id, s) -> save("res$id", s.name) }
        val s1 = preview.subscribeToEvents("pubsub", "requests", listener(::onRequest), TypeRef.STRING)
        preview.subscribeToEvents("pubsub", "release", listener(::onRelease), TypeRef.STRING)
        println("Resource Manager initialized")
        s1.awaitTermination()
    }
}