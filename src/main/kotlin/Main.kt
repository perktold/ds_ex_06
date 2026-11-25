
fun main(args: Array<String>) {
    when (args.getOrNull(0)) {
        "manager" -> Manager().run()
        "client" -> Client(args[1].toInt()).run()
        else -> error("usage: manager | client <n>")
    }
}