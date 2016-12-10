
lazy val root = (project in file(".")).
settings(
        name := "information_retrieval_2",
        version := "1.0",
        scalaVersion := "2.11.8"

        )

mainClass in (Compile,run) := Some("src.main.scala.SearchEngineZY")

libraryDependencies  ++= Seq(
      // other dependencies here
      "org.scalanlp" %% "breeze" % "0.12",
      "org.scalanlp" %% "breeze-natives" % "0.12",
      "org.scalanlp" %% "breeze-viz" % "0.12"
      )

resolvers ++= Seq(
      "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"

      )

// or 2.11.5
scalaVersion := "2.11.5"
