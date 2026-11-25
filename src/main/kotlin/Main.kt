#!/usr/bin/env kotlin

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.dapr.client.DaprClient
import io.dapr.client.DaprClientBuilder
import java.net.InetSocketAddress
import java.util.concurrent.Executors

const val PUBSUB = "pubsub"
const val STORE = "statestore"

data class Msg(var requestId: Int = 0, var clientId: Int = 0, var resourceId: Int = 0)
data class Resource(var status: String = "AVAILABLE", var heldBy: Int = 0, var requestId: Int = 0)
data class Req(var status: String = "PENDING", var clientId: Int = 0, var resourceId: Int = 0)
data class Queue(var ids: MutableList<Int> = mutableListOf())
data class Counter(var n: Int = 0)

val mapper = jacksonObjectMapper()

/** Startet einen HTTP-Server; Dapr holt /dapr/subscribe und liefert Events per POST an die Routen. */
fun startServer(port: Int, subs: Map<String, String>, handlers: Map<String, (Msg) -> Unit>) {
    val server = HttpServer.create(InetSocketAddress(port), 0)
    server.executor = Executors.newFixedThreadPool(8)

    server.createContext("/dapr/subscribe") { ex ->
        val body = subs.map { (topic, route) ->
            mapOf("pubsubname" to PUBSUB, "topic" to topic, "route" to route)
        }
        reply(ex, mapper.writeValueAsString(body))
    }
    handlers.forEach { (route, handler) ->
        server.createContext(route) { ex ->
            try {
                val event: JsonNode = mapper.readTree(ex.requestBody)
                handler(mapper.treeToValue(event["data"], Msg::class.java))
            } catch (e: Exception) {
                e.printStackTrace()
            }
            reply(ex, """{"status":"SUCCESS"}""")   // sonst liefert Dapr das Event erneut zu
        }
    }
    server.start()
}

fun reply(ex: HttpExchange, json: String) {
    val bytes = json.toByteArray()
    ex.responseHeaders.add("Content-Type", "application/json")
    ex.sendResponseHeaders(200, bytes.size.toLong())
    ex.responseBody.use { it.write(bytes) }
}

fun main() {
    val dapr = DaprClientBuilder().build()
    dapr.waitForSidecar(60000).block()
    val port = System.getenv("APP_PORT")!!.toInt()
    when (System.getenv("ROLE")) {
        "manager" -> Manager(dapr, port)
        "client" -> Client(dapr, port, System.getenv("CLIENT_ID")!!.toInt())
        else -> error("ROLE muss manager oder client sein")
    }
}

// ---------------- Resource Manager ----------------
class Manager(private val dapr: DaprClient, port: Int) {
    private val lock = Any()

    private inline fun <reified T : Any> get(key: String, default: T): T =
        dapr.getState(STORE, key, T::class.java).block()?.value ?: default
    private fun put(key: String, v: Any) { dapr.saveState(STORE, key, v).block() }

    init {
        synchronized(lock) {
            put("resource:1", Resource()); put("resource:2", Resource())
            put("pending-queue", Queue()); put("completed-count", Counter())
        }
        startServer(
            port,
            subs = mapOf("requests" to "/requests", "releases" to "/releases"),
            handlers = mapOf("/requests" to ::onRequest, "/releases" to ::onRelease)
        )
        println("Resource Manager initialized")
    }

    private fun onRequest(m: Msg) = synchronized(lock) {
        put("request:${m.requestId}", Req("PENDING", m.clientId))
        val q = get("pending-queue", Queue()); q.ids.add(m.requestId); put("pending-queue", q)
        grantPending()
        if (get("request:${m.requestId}", Req()).status == "PENDING")
            println("Request ${m.requestId} pending")
    }

    private fun onRelease(m: Msg) = synchronized(lock) {
        val req = get("request:${m.requestId}", Req())
        if (req.status == "COMPLETED") return@synchronized          // idempotent bei Redelivery
        put("resource:${m.resourceId}", Resource())                  // AVAILABLE
        req.status = "COMPLETED"; put("request:${m.requestId}", req)
        println("Client ${m.clientId} released Resource ${m.resourceId}")
        val c = get("completed-count", Counter()); c.n++; put("completed-count", c)
        grantPending()
        if (c.n == 6) println("All resource requests completed")
    }

    /** Nur aufrufen, während `lock` gehalten wird. */
    private fun grantPending() {
        val q = get("pending-queue", Queue())
        val it = q.ids.iterator()
        while (it.hasNext()) {
            val reqId = it.next()
            val resId = listOf(1, 2).firstOrNull { r -> get("resource:$r", Resource()).status == "AVAILABLE" } ?: break
            val req = get("request:$reqId", Req())
            put("resource:$resId", Resource("RESERVED", req.clientId, reqId))
            req.status = "GRANTED"; req.resourceId = resId; put("request:$reqId", req)
            it.remove()
            println("Resource $resId granted to Client ${req.clientId}")
            dapr.publishEvent(PUBSUB, "grants", Msg(reqId, req.clientId, resId)).block()
        }
        put("pending-queue", q)
    }
}

// ---------------- Client ----------------
class Client(private val dapr: DaprClient, port: Int, private val id: Int) {
    private val first = (id - 1) * 2 + 1       // Request-IDs: 1,2 / 3,4 / 5,6
    private val second = first + 1

    init {
        startServer(port, subs = mapOf("grants" to "/grants"), handlers = mapOf("/grants" to ::onGrant))
        println("Client $id initialized")
        Thread { Thread.sleep(5000); request(first) }.start()   // Manager zuerst abonnieren lassen
    }

    private fun request(reqId: Int) {
        dapr.publishEvent(PUBSUB, "requests", Msg(reqId, id)).block()
    }

    private fun onGrant(m: Msg) {
        if (m.clientId != id) return
        Thread {                                   // Callback nicht blockieren
            Thread.sleep((500..1500).random().toLong())
            dapr.publishEvent(PUBSUB, "releases", m).block()
            if (m.requestId == first) request(second)
        }.start()
    }
}