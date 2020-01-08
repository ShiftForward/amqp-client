import ReleaseTransformations._

name := "amqp-client"

organization := "eu.shiftforward"

scalaVersion := "2.12.10"

crossScalaVersions := Seq("2.11.12", "2.12.10", "2.13.1")

scalacOptions ++= Seq("-feature", "-language:postfixOps")

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= {
  val akkaVersion = "2.5.27"
  Seq(
    "com.rabbitmq"         % "amqp-client"          % "5.8.0",
    "com.typesafe.akka"    %% "akka-actor"          % akkaVersion % "provided",
    "com.typesafe.akka"    %% "akka-slf4j"          % akkaVersion % "test",
    "com.typesafe.akka"    %% "akka-testkit"        % akkaVersion % "test",
    "org.scalatest"        %% "scalatest"           % "3.0.8" % "test",
    "ch.qos.logback"       % "logback-classic"      % "1.2.3" % "test",
    "junit"           	   % "junit"                % "4.12" % "test"
  )
}

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true
publishArtifact in Test := false
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }

licenses := Seq("MIT License" -> url("http://www.opensource.org/licenses/mit-license.php"))
homepage := Some(url("https://github.com/ShiftForward/amqp-client"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/velocidi/amqp-client.git"),
    "scm:git:git@github.com:velocidi/amqp-client.git"
  )
)

releaseTagComment := s"Release ${(version in ThisBuild).value}"
releaseCommitMessage := s"Set version to ${(version in ThisBuild).value}"

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  releaseStepCommandAndRemaining("+test"),
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  releaseStepCommandAndRemaining("+publishSigned"),
  setNextVersion,
  commitNextVersion,
  pushChanges,
  releaseStepCommandAndRemaining("sonatypeReleaseAll"))

pomExtra := {
  <developers>
    <developer>
      <id>sstone</id>
      <name>Fabrice Drouin</name>
      <roles>
        <role>developer</role>
      </roles>
    </developer>
  </developers>
}
