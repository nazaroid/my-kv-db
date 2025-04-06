package git.plugin

import java.time.{LocalDateTime, ZoneOffset}

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.lib.Constants._
import org.eclipse.jgit.lib.{ObjectId, Repository}
import org.eclipse.jgit.revwalk.RevCommit
import org.eclipse.jgit.storage.file.FileRepositoryBuilder

import scala.collection.JavaConverters._

final case class GitVersion(
  buildDate: LocalDateTime,
  gitBranch: String,
  gitRepoIsClean: Boolean,
  gitHeadCommit: String,
  gitHeadCommitMsg: Option[String],
  gitCommitAuthor: Option[String],
  gitCommitDate: Option[LocalDateTime]
)

object GitVersion {
  def apply(): GitVersion = apply(new FileRepositoryBuilder().readEnvironment.findGitDir.build)

  def apply(repository: Repository): GitVersion = {
    val git = new Git(repository)

    val headId = repository.findRef(HEAD).getObjectId
    val headIdStr = ObjectId.toString(headId)
    val headCommit = git.log().add(headId).setMaxCount(1).call().asScala.toSeq.headOption
    val repoIsClean: Boolean = git.status.call.isClean
    val commitDateTime: Option[LocalDateTime] =
      headCommit.map(r => LocalDateTime.ofEpochSecond(r.getCommitTime.toLong, 0, ZoneOffset.UTC))
    val commitAuthorName: Option[String] = headCommit.map(_.getCommitterIdent.getName)
    val headCommitMsg = headCommit.map(_.getFullMessage)

    GitVersion(
      buildDate = LocalDateTime.now,
      gitBranch = repository.getBranch,
      gitRepoIsClean = repoIsClean,
      gitHeadCommit = headIdStr,
      gitHeadCommitMsg = headCommitMsg,
      gitCommitAuthor = commitAuthorName,
      gitCommitDate = commitDateTime
    )
  }
}
