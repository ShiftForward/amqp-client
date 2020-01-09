package com.github.sstone.amqp

import org.scalatestplus.junit.JUnitRunner
import org.junit.runner.RunWith
import akka.testkit.TestProbe
import akka.actor.{Actor, DeadLetter, Props}
import akka.pattern.gracefulStop
import java.util.concurrent.{CountDownLatch, TimeUnit}

import concurrent.duration._
import com.rabbitmq.client.AMQP.{Exchange, Queue}
import com.github.sstone.amqp.Amqp._
import com.rabbitmq.client.GetResponse
import com.github.sstone.amqp.ChannelOwner.NotConnectedError

import scala.collection.immutable.Set
import scala.concurrent.Await

@RunWith(classOf[JUnitRunner])
class ChannelOwnerSpec extends ChannelSpec {
  "ChannelOwner" should {

    "implement basic error handling" in {
      channelOwner ! DeclareQueue(QueueParameters("no_such_queue", passive = true))
      expectMsgClass(1 second, classOf[Amqp.Error])
    }

    "allow users to create, bind, get from, purge and delete queues" in {
      val queue = "my_test_queue"

      // declare a queue, bind it to "my_test_key" on "amq.direct" and publish a message
      channelOwner ! DeclareQueue(QueueParameters(queue, passive = false, durable = false, autodelete = true))
      channelOwner ! QueueBind(queue, "amq.direct", Set("my_test_key"))
      channelOwner ! Publish("amq.direct", "my_test_key", "yo!".getBytes)
      receiveN(3, 2 seconds)
      Thread.sleep(100)

      // check that there is 1 message in the queue
      channelOwner ! DeclareQueue(QueueParameters(queue, passive = true))
      val Amqp.Ok(_, Some(check1: Queue.DeclareOk)) = receiveOne(1 second)

      // receive from the queue
      channelOwner ! Get(queue, true)
      val Amqp.Ok(_, Some(msg: GetResponse)) = receiveOne(1 second)
      assert(new String(msg.getBody) == "yo!")

      // purge the queue
      channelOwner ! PurgeQueue(queue)
      receiveOne(1 second)

      // check that there are no more messages in the queue
      channelOwner ! DeclareQueue(QueueParameters(queue, passive = true))
      val Amqp.Ok(_, Some(check2: Queue.DeclareOk)) = receiveOne(1 second)

      // delete the queue
      channelOwner ! DeleteQueue(queue)
      val Amqp.Ok(_, Some(check3: Queue.DeleteOk)) = receiveOne(1 second)

      assert(check1.getMessageCount === 1)
      assert(check2.getMessageCount === 0)
    }

    "allow users to create, bind, and delete exchanges" in {
      val source = "my_source_exchange"
      val destination = "my_destination_exchange"
      val queue = "my_destination_queue"
      val routingKey = "my_test_key"

      // declare source and destination exchanges, bound together via "my_test_key"
      channelOwner ! DeclareExchange(ExchangeParameters(source, passive = false, exchangeType = "direct", durable = false, autodelete = true))
      val Amqp.Ok(_, Some(_: Exchange.DeclareOk)) = receiveOne(1 second)
      channelOwner ! DeclareExchange(ExchangeParameters(destination, passive = false, exchangeType = "direct", durable = false, autodelete = true))
      val Amqp.Ok(_, Some(_: Exchange.DeclareOk)) = receiveOne(1 second)
      channelOwner ! ExchangeBind(destination, source, Set(routingKey))
      val Amqp.Ok(_, Some(_)) = receiveOne(1 second)
      // declare destination queue bound in destination exchange via "my_test_key"
      channelOwner ! DeclareQueue(QueueParameters(queue, passive = false, durable = false, autodelete = true))
      val Amqp.Ok(_, Some(_: Queue.DeclareOk)) = receiveOne(1 second)
      channelOwner ! QueueBind(queue, destination, Set(routingKey))
      val Amqp.Ok(_, Some(_)) = receiveOne(1 second)

      // publish to the source exchange
      channelOwner ! Publish(source, routingKey, "yo!".getBytes)
      val Amqp.Ok(_, None) = receiveOne(2 seconds)

      Thread.sleep(100) // give the server a chance to route the message

      // check that there is 1 message in the destination queue in the destination exchange
      channelOwner ! DeclareQueue(QueueParameters(queue, passive = true))
      val Amqp.Ok(_, Some(check1: Queue.DeclareOk)) = receiveOne(1 second)

      // receive from the queue
      channelOwner ! Get(queue, autoAck = true)
      val Amqp.Ok(_, Some(msg: GetResponse)) = receiveOne(1 second)
      assert(new String(msg.getBody) == "yo!")

      // purge the queue
      channelOwner ! PurgeQueue(queue)
      receiveOne(1 second)

      // check that there are no more messages in the queue
      channelOwner ! DeclareQueue(QueueParameters(queue, passive = true))
      val Amqp.Ok(_, Some(check2: Queue.DeclareOk)) = receiveOne(1 second)

      // delete the queue and exchanges
      channelOwner ! DeleteQueue(queue)
      val Amqp.Ok(_, Some(_: Queue.DeleteOk)) = receiveOne(1 second)
      channelOwner ! DeleteExchange(destination)
      val Amqp.Ok(_, Some(_: Exchange.DeleteOk)) = receiveOne(1 second)
      channelOwner ! DeleteExchange(source)
      val Amqp.Ok(_, Some(_: Exchange.DeleteOk)) = receiveOne(1 second)

      assert(check1.getMessageCount === 1)
      assert(check2.getMessageCount === 0)
    }
  }

  "return unroutable messages" in {
    channelOwner ! AddReturnListener(self)
    val Amqp.Ok(_, None) = receiveOne(1 seconds)
    channelOwner ! Publish("", "no_such_queue", "test".getBytes)
    val Amqp.Ok(_, None) = receiveOne(1 seconds)
    expectMsgClass(1 seconds, classOf[ReturnedMessage])
  }

  "register status listeners" in {
    val probe1 = TestProbe()
    val probe2 = TestProbe()
    channelOwner ! AddStatusListener(probe1.ref)
    channelOwner ! AddStatusListener(probe2.ref)
    probe1.expectMsg(ChannelOwner.Connected)
    probe2.expectMsg(ChannelOwner.Connected)
    system.stop(probe1.ref)
    system.stop(probe2.ref)
  }

  "remove a status listener when it terminates" in {
    val latch = new CountDownLatch(1)

    val statusListenerProbe = system.actorOf(Props(new Actor {
      def receive = {
        case ChannelOwner.Connected => latch.countDown()
        case _ => fail("Status listener did not detect channel connection")
      }
    }))

    latch.await(3, TimeUnit.SECONDS)
    channelOwner ! AddStatusListener(statusListenerProbe)

    val deadletterProbe = TestProbe()
    system.eventStream.subscribe(deadletterProbe.ref, classOf[DeadLetter])

    Await.result(gracefulStop(statusListenerProbe, 5 seconds), 6 seconds)

    channelOwner ! DeclareQueue(QueueParameters("NO_SUCH_QUEUE", passive = true))
    expectMsgClass(classOf[Amqp.Error])
    deadletterProbe.expectNoMessage(1 second)
  }

  "return requests when not connected" in {
    val probe = TestProbe()
    channelOwner ! AddStatusListener(probe.ref)
    probe.expectMsg(ChannelOwner.Connected)

    // Force channel to close by inducing an error
    channelOwner ! DeclareQueue(QueueParameters("NO_SUCH_QUEUE", passive = true))

    expectMsgPF() {
      case Error(DeclareQueue(QueueParameters(_,_, _, _, _, _)),_) => true
    }

    probe.expectMsg(ChannelOwner.Disconnected)

    val testRequest = DeclareQueue(QueueParameters("my_test_queue", passive = false))
    channelOwner ! testRequest

    // we also test for Ok() here because it is possible, though very unlikely, that the
    // ChannelOwner had already received a new channel before it got our test request
    expectMsgPF() {
      case NotConnectedError(testRequest) => true
      case Ok(testRequest, _) => true
    }
  }

  "Multiple ChannelOwners" should {
    "each transition from Disconnected to Connected when they receive a channel" in {
      val concurrent = 10
      val actors = for (i <- 1 until concurrent) yield ConnectionOwner.createChildActor(conn, ChannelOwner.props(), name = Some(s"$i-instance"))
      val latch = waitForConnection(system, actors: _*)
      latch.await(10000, TimeUnit.MILLISECONDS)
      latch.getCount should be(0)
    }
  }
}
