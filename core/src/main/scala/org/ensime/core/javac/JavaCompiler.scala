// Copyright: 2010 - 2017 https://github.com/ensime/ensime-server/graphs
// License: http://www.gnu.org/licenses/gpl-3.0.en.html
package org.ensime.core.javac

import java.io.{ File => JFile }
import java.nio.charset.Charset
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.Locale
import java.util.zip.ZipFile

import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._

import akka.actor.ActorRef
import akka.event.slf4j.SLF4JLogging
import com.sun.source.tree.Scope
import com.sun.source.util.{ JavacTask, TreePath }
import com.sun.tools.javac.api.JavacTool
import com.sun.tools.javac.file.RelativePath.RelativeFile;
import com.sun.tools.javac.file.ZipArchive
import com.sun.tools.javac.util.Abort
import javax.tools._
import org.ensime.api._
import org.ensime.core.DocSigPair
import org.ensime.config.richconfig._
import org.ensime.indexer.{ FullyQualifiedName, SearchService }
import org.ensime.util.ReportHandler
import org.ensime.util.file._
import org.ensime.util.ensimefile._
import org.ensime.vfs._

class JavaCompiler(
  val config: EnsimeConfig,
  val reportHandler: ReportHandler,
  val indexer: ActorRef,
  val search: SearchService,
  val vfs: EnsimeVFS
) extends JavaDocFinding
    with JavaCompletionsAtPoint
    with JavaTypeAtPoint
    with JavaSymbolAtPoint
    with JavaSourceFinding
    with Helpers
    with SLF4JLogging {

  private val listener = new JavaDiagnosticListener()
  private val silencer = new SilencedDiagnosticListener()
  private val cp = config.classpath.mkString(JFile.pathSeparator)
  private val workingSet = new ConcurrentHashMap[String, JavaFileObject]()
  private val archiveCache = new ConcurrentHashMap[Path, ZipArchive]()

  private implicit def charset = Charset.defaultCharset() // how can we infer this?

  lazy val compiler = JavacTool.create()
  lazy val fileManager = compiler.getStandardFileManager(listener, null, charset)

  def getTask(
    lint: String,
    listener: DiagnosticListener[JavaFileObject],
    files: java.lang.Iterable[JavaFileObject]
  ): JavacTask = {
    compiler.getTask(null, fileManager, listener, List(
      "-cp", cp, "-Xlint:" + lint, "-proc:none"
    ).asJava, null, files).asInstanceOf[JavacTask]
  }

  def createJavaFileObject(sf: SourceFileInfo): JavaFileObject = sf match {
    case SourceFileInfo(f: RawFile, None, None, _) =>
      new JavaObjectEnsimeFile(f, None)
    case SourceFileInfo(f: RawFile, None, Some(contentsIn), _) =>
      new JavaObjectEnsimeFile(f, Some(contentsIn.readString))
    case SourceFileInfo(f: RawFile, contents, _, _) =>
      new JavaObjectEnsimeFile(f, contents)
    case SourceFileInfo(ArchiveFile(jar, entry), _, _, _) =>
      val rf = new RelativeFile(entry.dropWhile(_ == '/'))
      archiveCache.computeIfAbsent(
        jar,
        { jar: Path => new ZipArchive(fileManager, new ZipFile(jar.toFile)) }.asJava
      ).getFileObject(rf.dirname, rf.basename)
  }

  def internSource(sf: SourceFileInfo): JavaFileObject = {
    val jfo = createJavaFileObject(sf)
    workingSet.put(sf.file.uriString, jfo)
    jfo
  }

  def askTypecheckFiles(files: List[SourceFileInfo]): Unit = {
    reportHandler.clearAllJavaNotes()
    for (sf <- files) {
      internSource(sf)
    }
    typecheckAll()
  }

  def askLinkPos(fqn: FullyQualifiedName, file: SourceFileInfo): Option[SourcePosition] = {
    val infos = typecheckForUnits(List(file))
    infos.headOption.flatMap { c => findInCompiledUnit(c, fqn) }
  }

  def askDocSignatureAtPoint(file: SourceFileInfo, offset: Int): Option[DocSigPair] = {
    pathToPoint(file, offset) flatMap {
      case (c: Compilation, path: TreePath) =>
        docSignature(c, path)
    }
  }

  protected def pathToPoint(file: SourceFileInfo, offset: Int): Option[(Compilation, TreePath)] = {
    val infos = typecheckForUnits(List(file))
    infos.headOption.flatMap { c =>
      val path: Option[TreePath] = PathFor(c, offset)
      path.map { p => (c, p) }
    }
  }

  protected def scopeForPoint(file: SourceFileInfo, offset: Int): Option[(Compilation, Scope)] = {
    val infos = typecheckForUnits(List(file))
    infos.headOption.flatMap { c =>
      val path: Option[Scope] = ScopeFor(c, offset)
      path.map { p => (c, p) }
    }
  }

  private def typecheckAll(): Unit = {
    val task = getTask("all", listener, workingSet.values)
    val t = System.currentTimeMillis()
    try {
      task.parse()
      task.analyze()
      log.info("Parsed and analyzed: " + (System.currentTimeMillis() - t) + "ms")
    } catch {
      case e @ (_: Abort | _: ArrayIndexOutOfBoundsException | _: AssertionError) =>
        log.error("Javac error: " + e.getMessage())
    }
  }

  private def typecheckForUnits(inputs: List[SourceFileInfo]): Vector[Compilation] = {
    // We only want the compilation units for inputs, but we need to typecheck them w.r.t
    // the full working set.
    val inputJfos = inputs.map { sf => internSource(sf).toUri }.toSet
    val task = getTask("none", silencer, workingSet.values)
    val t = System.currentTimeMillis()

    try {
      val units = task.parse().asScala.filter(
        unit => inputJfos.contains(
          unit.getSourceFile.toUri
        )
      ).map(Compilation(task, _)).toVector

      task.analyze()
      log.info("Parsed and analyzed for trees: " + (System.currentTimeMillis() - t) + "ms")
      units
    } catch {
      case e @ (_: Abort | _: ArrayIndexOutOfBoundsException | _: AssertionError) =>
        log.error("Javac error: " + e.getMessage())
        Vector()
    }
  }

  private class JavaObjectEnsimeFile(
      val f: EnsimeFile,
      val contents: Option[String]
  ) extends SimpleJavaFileObject(f.uri, JavaFileObject.Kind.SOURCE) {
    override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence =
      contents.getOrElse(f.readStringDirect)
  }

  private class JavaDiagnosticListener extends DiagnosticListener[JavaFileObject] with ReportHandler {
    def report(diag: Diagnostic[_ <: JavaFileObject]): Unit = {
      reportHandler.reportJavaNotes(List(
        Note(
          diag.getSource().getName(),
          diag.getMessage(Locale.ENGLISH),
          diag.getKind() match {
            case Diagnostic.Kind.ERROR => NoteError
            case Diagnostic.Kind.WARNING => NoteWarn
            case Diagnostic.Kind.MANDATORY_WARNING => NoteWarn
            case _ => NoteInfo
          },
          diag.getStartPosition() match {
            case x if x > -1 => x.toInt
            case _ => diag.getPosition().toInt
          },
          diag.getEndPosition().toInt,
          diag.getLineNumber().toInt,
          diag.getColumnNumber().toInt
        )
      ))
    }
  }

  private class SilencedDiagnosticListener extends DiagnosticListener[JavaFileObject] with ReportHandler {
    def report(diag: Diagnostic[_ <: JavaFileObject]): Unit = {}
  }
}
