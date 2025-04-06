package git.plugin

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import sbt.Keys._
import sbt._

object CustomGit extends AutoPlugin {

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")

  def gitVersionSettings(filename: String): Seq[Setting[_]] =
    inConfig(Compile)(
      Seq(
        resourceGenerators += generateVersion(
          resourceManaged,
          _ / filename,
          """
        |git.commit.id=%s
        |git.commit.message.full=%s
        |build.date=%s
        |git.branch=%s
        |git.repo-is-clean=%s
        |git.head.commit=%s
        |git.head.commit.author=%s
        |git.head.commit.date=%s
        |"""
        ).taskValue
      ))

  private def generateVersion(dir: SettingKey[File], locate: File => File, template: String) = Def.task[Seq[File]] {
    val gitVersion = GitVersion()
    val file = locate(dir.value)
    val content = template.stripMargin.format(
      gitVersion.gitHeadCommit,
      gitVersion.gitHeadCommitMsg.getOrElse(""),
      formatDate(gitVersion.buildDate),
      gitVersion.gitBranch,
      gitVersion.gitRepoIsClean.toString,
      gitVersion.gitHeadCommit,
      gitVersion.gitCommitAuthor.getOrElse(""),
      gitVersion.gitCommitDate.map(formatDate).getOrElse("")
    )
    if (!file.exists || IO.read(file) != content) IO.write(file, content)
    Seq(file)
  }

  private def formatDate(date: LocalDateTime) = date.format(formatter)

}
