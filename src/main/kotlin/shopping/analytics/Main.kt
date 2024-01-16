package shopping.analytics

import akka.actor.typed.ActorSystem
import akka.actor.typed.javadsl.Behaviors
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.javadsl.AkkaManagement
import org.slf4j.Logger
import org.slf4j.LoggerFactory


object Main {
    private val logger: Logger = LoggerFactory.getLogger(Main::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val system = ActorSystem.create(Behaviors.empty<Void>(), "ShoppingAnalyticsService")
        try {
            init(system)
        } catch (e: Exception) {
            logger.error("Terminating due to initialization failure.", e)
            system.terminate()
        }
    }

    fun init(system: ActorSystem<Void>?) {
        AkkaManagement.get(system).start()
        ClusterBootstrap.get(system).start()

        ShoppingCartEventConsumer.init(system!!)
    }
}
