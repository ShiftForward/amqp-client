package com.github.sstone.amqp

import convert.Converters._

import com.rabbitmq.client.AMQP.BasicProperties
import com.rabbitmq.client.{Delivery => _, _}
import akka.actor._
import com.github.sstone.amqp.Amqp._
import scala.util.Try
import scala.util.Failure
import scala.util.Success

import akka.event.LoggingReceive
import scala.collection.mutable
import java.util.UUID

object ChannelOwner {

  sealed trait State

  case object Disconnected extends State

  case object Connected extends State

  case class NotConnectedError(request: Request)

  def props(init: Seq[Request] = Seq.empty[Request], channelParams: Option[ChannelParameters] = None): Props = Props(new ChannelOwner(init, channelParams))

  private[amqp] class Forwarder(channel: Channel) extends Actor with ActorLogging {

    override def postStop(): Unit = {
      Try(channel.close())
    }

    override def unhandled(message: Any): Unit = log.warning(s"unhandled message $message")

    def receive = {
      case request@AddShutdownListener(listener) => {
        sender ! withChannel(channel, request)(c => c.addShutdownListener(new ShutdownListener {
          def shutdownCompleted(cause: ShutdownSignalException): Unit = {
            listener ! Shutdown(cause)
          }
        }))
      }
      case request@AddReturnListener(listener) => {
        sender ! withChannel(channel, request)(c => c.addReturnListener(new ReturnListener {
          def handleReturn(replyCode: Int, replyText: String, exchange: String, routingKey: String, properties: BasicProperties, body: Array[Byte]): Unit = {
            listener ! ReturnedMessage(replyCode, replyText, exchange, routingKey, properties, body)
          }
        }))
      }
      case request@Publish(exchange, routingKey, body, properties, mandatory, immediate) => {
        log.debug("publishing %s".format(request))
        val props = properties getOrElse new AMQP.BasicProperties.Builder().build()
        sender ! withChannel(channel, request)(c => c.basicPublish(exchange, routingKey, mandatory, immediate, props, body))
      }
      case request@Transaction(publish) => {
        sender ! withChannel(channel, request) {
          c => {
            c.txSelect()
            publish.foreach(p => c.basicPublish(p.exchange, p.key, p.mandatory, p.immediate, p.properties getOrElse new AMQP.BasicProperties.Builder().build(), p.body))
            c.txCommit()
          }
        }
      }
      case request@DeclareExchange(exchange) => {
        log.debug("declaring exchange {}", exchange)
        sender ! withChannel(channel, request)(c => declareExchange(c, exchange))
      }
      case request@DeleteExchange(exchange, ifUnused) => {
        log.debug("deleting exchange {} ifUnused {}", exchange, ifUnused)
        sender ! withChannel(channel, request)(c => c.exchangeDelete(exchange, ifUnused))
      }
      case request@DeclareQueue(queue) => {
        log.debug("declaring queue {}", queue)
        sender ! withChannel(channel, request)(c => declareQueue(c, queue))
      }
      case request@PurgeQueue(queue) => {
        log.debug("purging queue {}", queue)
        sender ! withChannel(channel, request)(c => c.queuePurge(queue))
      }
      case request@DeleteQueue(queue, ifUnused, ifEmpty) => {
        log.debug("deleting queue {} ifUnused {} ifEmpty {}", queue, ifUnused, ifEmpty)
        sender ! withChannel(channel, request)(c => c.queueDelete(queue, ifUnused, ifEmpty))
      }
      case request@ExchangeBind(destination, source, routingKeys, args) => {
        log.debug("binding exchange {} to keys {} on exchange {}", destination, routingKeys.mkString(", "), source)
        sender ! withChannel(channel, request)(c => routingKeys.map(rk => c.exchangeBind(destination, source, rk, args.asJava)))
      }
      case request@QueueBind(queue, exchange, routingKeys, args) => {
        log.debug("binding queue {} to keys {} on exchange {}", queue, routingKeys.mkString(", "), exchange)
        sender ! withChannel(channel, request)(c => routingKeys.map(rk => c.queueBind(queue, exchange, rk, args.asJava)))
      }
      case request@QueueUnbind(queue, exchange, routingKey, args) => {
        log.debug("unbinding queue {} to key {} on exchange {}", queue, routingKey, exchange)
        sender ! withChannel(channel, request)(c => c.queueUnbind(queue, exchange, routingKey, args.asJava))
      }
      case request@Get(queue, autoAck) => {
        log.debug("getting from queue {} autoAck {}", queue, autoAck)
        sender ! withChannel(channel, request)(c => c.basicGet(queue, autoAck))
      }
      case request@Ack(deliveryTag) => {
        log.debug("acking %d on %s".format(deliveryTag, channel))
        sender ! withChannel(channel, request)(c => c.basicAck(deliveryTag, false))
      }
      case request@Reject(deliveryTag, requeue) => {
        log.debug("rejecting %d on %s".format(deliveryTag, channel))
        sender ! withChannel(channel, request)(c => c.basicReject(deliveryTag, requeue))
      }
      case request@CreateConsumer(listener) => {
        log.debug(s"creating new consumer for listener $listener")
        sender ! withChannel(channel, request)(c => new DefaultConsumer(channel) {
          override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]): Unit = {
            listener ! Delivery(consumerTag, envelope, properties, body)
          }
        })
      }
      case request:ConfirmSelect.type => {
        sender ! withChannel(channel, request)(c => c.confirmSelect())
      }
      case request@AddConfirmListener(listener) => {
        sender ! withChannel(channel, request)(c => c.addConfirmListener(new ConfirmListener {
          def handleAck(deliveryTag: Long, multiple: Boolean): Unit = listener ! HandleAck(deliveryTag, multiple)

          def handleNack(deliveryTag: Long, multiple: Boolean): Unit = listener ! HandleNack(deliveryTag, multiple)
        }))
      }
      case request@WaitForConfirms(timeout) => {
        sender ! withChannel(channel, request)(c => timeout match {
          case Some(value) => c.waitForConfirms(value)
          case None => c.waitForConfirms()
        })
      }
      case request@WaitForConfirmsOrDie(timeout) => {
        sender ! withChannel(channel, request)(c => timeout match {
          case Some(value) => c.waitForConfirmsOrDie(value)
          case None => c.waitForConfirmsOrDie()
        })
      }
    }
  }

  def withChannel[T](channel: Channel, request: Request)(f: Channel => T) = {
    Try(f(channel)) match {
      case Success(()) => {
        Ok(request)
      }
      case Success(result) => {
        Ok(request, Some(result))
      }
      case Failure(cause) => {
        Amqp.Error(request, cause)
      }
    }
  }
}

class ChannelOwner(init: Seq[Request] = Seq.empty[Request], channelParams: Option[ChannelParameters] = None) extends Actor with ActorLogging {

  import ChannelOwner._

  var requestLog: Vector[Request] = init.toVector
  val statusListeners = mutable.HashSet.empty[ActorRef]

  override def preStart() = context.parent ! ConnectionOwner.CreateChannel

  override def unhandled(message: Any): Unit = message match {
    case Terminated(actor) if statusListeners.contains(actor) => {
      context.unwatch(actor)
      statusListeners.remove(actor)
    }
    case _ => {
      log.warning(s"unhandled message $message")
      super.unhandled(message)
    }
  }

  def onChannel(channel: Channel, forwarder: ActorRef): Unit = {
    channelParams.foreach(p => channel.basicQos(p.qos, p.global))
  }

  def receive = disconnected

  def newChannel(channel: Channel, previousForwarder: Option[ActorRef]) = {
    log.info(s"got channel $channel")
    previousForwarder.foreach { forwarder =>
      forwarder ! PoisonPill
      statusListeners.foreach(_ ! Disconnected)
    }
    val forwarder = context.actorOf(Props(new Forwarder(channel)), name = "forwarder-" + UUID.randomUUID.toString)
    forwarder ! AddShutdownListener(self)
    forwarder ! AddReturnListener(self)
    Try(onChannel(channel, forwarder)) match {
      case Success(_) =>
        requestLog.foreach(r => self forward r)
        statusListeners.foreach(_ ! Connected)
        context.become(connected(channel, forwarder))
      case Failure(exception) =>
        log.error(exception, "onChannel")
        context.stop(forwarder)
        context.parent ! ConnectionOwner.CreateChannel
        context.become(disconnected)
    }
  }

  def disconnected: Receive = LoggingReceive {
    case channel: Channel =>
      newChannel(channel, None)
    case Record(request: Request) =>
      requestLog :+= request
    case AddStatusListener(actor) =>
      addStatusListener(actor)
    case request: Request =>
      sender ! NotConnectedError(request)
  }

  def connected(channel: Channel, forwarder: ActorRef): Receive = LoggingReceive {
    case Amqp.Ok(_, _) => ()
    case Record(request: Request) =>
      requestLog :+= request
      self forward request
    case AddStatusListener(listener) =>
      addStatusListener(listener)
      listener ! Connected
    case request: Request =>
      forwarder forward request
    case c: Channel =>
      newChannel(c, Some(forwarder))
    case Shutdown(cause) if !cause.isInitiatedByApplication => {
      log.error(cause, "shutdown")
      context.stop(forwarder)
      // If we get a connection closed, either via the `ConnectionOwner` or directly through the channel, we don't need
      // to request a new channel, since a new one will be created when the `ConnectionOwner` tries to reconnect.
      if (!cause.isHardError)
        context.parent ! ConnectionOwner.CreateChannel
      statusListeners.foreach(_ ! Disconnected)
      context.become(disconnected)
    }
  }

  private def addStatusListener(listener: ActorRef): Unit = {
    if (!statusListeners.contains(listener)) {
      context.watch(listener)
      statusListeners.add(listener)
    }
  }
}
