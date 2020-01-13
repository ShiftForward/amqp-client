package com.github.sstone.amqp

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import akka.testkit.TestProbe

import concurrent.duration._
import com.rabbitmq.client.AMQP.BasicProperties
import com.github.sstone.amqp.Amqp._
import com.github.sstone.amqp.Amqp.Publish
import com.github.sstone.amqp.Amqp.ExchangeParameters
import com.github.sstone.amqp.Amqp.Binding
import com.github.sstone.amqp.Amqp.Delivery
import com.rabbitmq.client.AMQP

@RunWith(classOf[JUnitRunner])
class ProducerSpec extends ChannelSpec {
  "Producers" should {
    "be able to specify custom message properties" in {
      val exchange = StandardExchanges.amqDirect
      val queue = randomQueue
      val routingKey = randomKey
      val probe = TestProbe()
      val consumer = ConnectionOwner.createChildActor(conn, Consumer.props(listener = Some(probe.ref)), timeout = 5000 millis, name = Some("ProducerSpec.consumer"))
      val producer = ConnectionOwner.createChildActor(conn, ChannelOwner.props(), timeout = 5000 millis, name = Some("ProducerSpec.producer"))
      waitForConnection(system, conn, consumer, producer).await()

      // create a queue, bind it to "my_key" and consume from it
      consumer ! AddBinding(Binding(exchange, queue, Set(routingKey)))

      fishForMessage(1 second) {
        case Amqp.Ok(AddBinding(Binding(`exchange`, `queue`, routingKeys)), _) if routingKeys == Set(routingKey) => true
        case msg => {
          println(s"unexpected $msg")
          false
        }
      }

      val message = "yo!".getBytes
      producer ! Publish(exchange.name, routingKey, message, Some(new BasicProperties.Builder().contentType("my content").build()))

      val delivery = probe.receiveOne(1.second).asInstanceOf[Delivery]
      assert(delivery.properties.getContentType === "my content")
    }
    "publish messages within an AMQP transaction" in  {
      val exchange = StandardExchanges.amqDirect
      val queue = randomQueue
      val routingKey = randomKey
      val probe = TestProbe()
      val consumer = ConnectionOwner.createChildActor(conn, Consumer.props(listener = Some(probe.ref)), timeout = 5000 millis)
      val producer = ConnectionOwner.createChildActor(conn, ChannelOwner.props())
      waitForConnection(system, conn, consumer, producer).await()

      // create a queue, bind it to our routing key and consume from it
      consumer ! AddBinding(Binding(exchange, queue, Set(routingKey)))

      fishForMessage(1 second) {
        case Amqp.Ok(AddBinding(Binding(`exchange`, `queue`, tal)), _) => true
        case _ => false
      }

      val message = "yo!".getBytes
      val props = new BasicProperties.Builder().contentType("my content").contentEncoding("my encoding").build()
      producer ! Transaction(
        List(
          Publish(exchange.name, routingKey, message, properties = Some(props)),
          Publish(exchange.name, routingKey, message, properties = Some(props)),
          Publish(exchange.name, routingKey, message, properties = Some(props))))

      var received = List[Delivery]()
      probe.receiveWhile(2.seconds) {
        case message: Delivery => received = message :: received
      }
      assert(received.length === 3)
      received.foreach(m => {
        assert(m.properties.getContentEncoding === "my encoding")
        assert(m.properties.getContentType === "my content")
      })
    }
    "be able to dead letter but not be able to directly publish to internal exchanges" in {
      val dlx = ExchangeParameters(name = randomExchangeName, passive = false, autodelete = true, internal = true, exchangeType = "direct")
      val exchange = ExchangeParameters(name = randomExchangeName, passive = false, autodelete = true, internal = false, exchangeType = "direct", args = Map("alternate-exchange" -> dlx.name))
      val dlq = randomQueue
      val routingKey = randomKey
      val probe = TestProbe()
      val consumer = ConnectionOwner.createChildActor(conn, Consumer.props(listener = Some(probe.ref)), timeout = 5000 millis, name = Some("ProducerSpec.consumer"))
      val producer = ConnectionOwner.createChildActor(conn, ChannelOwner.props(), timeout = 5000 millis, name = Some("ProducerSpec.producer"))
      waitForConnection(system, conn, consumer, producer).await()

      // create a dead letter queue, bind it to "my_key" and consume from it
      val dlxBinding = AddBinding(Binding(dlx, dlq, Set(routingKey)))
      consumer ! dlxBinding
      // create a queue/binding in the main exchange such that any publish gets routed to the alternate exchange but still triggers auto-deletion
      consumer ! DeclareExchange(exchange)
      val binding = AddBinding(Binding(exchange, QueueParameters("unused", passive = false, durable = false, autodelete = true), Set.empty))
      consumer ! binding

      fishForMessage(1 second) {
        case Amqp.Ok(`dlxBinding`, _) => true
        case msg => {
          println(s"unexpected $msg")
          false
        }
      }
      fishForMessage(1 second) {
        case Amqp.Ok(`binding`, _) => true
        case msg => {
          println(s"unexpected $msg")
          false
        }
      }

      val message = "nope!".getBytes
      val properties = new BasicProperties.Builder().contentType("my content").build()

      producer ! Publish(exchange.name, routingKey, message, Some(properties))

      // message should have been routed to dlx
      probe.expectMsgPF() {
        case d @ Delivery(_, env, `properties`, `message`) =>
          if(env.getExchange == dlx.name) {
            true
          } else {
            println(s"unexpected $d")
            false
          }
        case msg => {
          println(s"unexpected $msg")
          false
        }
      }

      val Ref = probe.ref
      producer ! AddShutdownListener(Ref)
      fishForMessage(1 second) {
        case Amqp.Ok(AddShutdownListener(Ref), _) => true
        case msg => {
          println(s"unexpected $msg")
          false
        }
      }

      // publishing directly to internal exchange should fail
      producer ! Publish(dlx.name, routingKey, message, Some(properties))
      probe.expectMsgPF(1 second) {
        case Shutdown(sse) => sse.getReason match {
          case close: AMQP.Channel.Close if close.getReplyCode == AMQP.ACCESS_REFUSED => true
          case msg =>
            println(s"unexpected $msg")
            false
        }
        case msg => {
          println(s"unexpected $msg")
          false
        }
      }
    }
  }
}
