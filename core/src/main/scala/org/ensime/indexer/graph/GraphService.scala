// Copyright: 2010 - 2017 https://github.com/ensime/ensime-server/graphs
// License: http://www.gnu.org/licenses/gpl-3.0.en.html
package org.ensime.indexer.graph

import java.sql.Timestamp
import java.util.concurrent.{ Executors, ThreadFactory, TimeUnit }

import scala.Predef._
import scala.collection.mutable
import scala.concurrent._
import scala.util.Try

import akka.event.slf4j.SLF4JLogging
import com.orientechnologies.orient.core.Orient
import com.orientechnologies.orient.core.config.OGlobalConfiguration
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal
import com.orientechnologies.orient.core.metadata.schema.OType
import com.tinkerpop.blueprints.Vertex
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory
import org.apache.commons.vfs2.FileObject
import org.ensime.api.{ DeclaredAs, EnsimeFile }
import org.ensime.indexer._
import org.ensime.indexer.IndexService.FqnIndex
import org.ensime.indexer.SearchService._
import org.ensime.indexer.orientdb.api._
import org.ensime.indexer.orientdb.schema.api._
import org.ensime.indexer.orientdb.syntax._
import org.ensime.util.ensimefile._
import org.ensime.util.file._
import org.ensime.util.fileobject._
import org.ensime.util.stringymap.api.BigDataFormat
import org.ensime.vfs._
import shapeless.cachedImplicit

// I'm not particularly keen on this kind of OOP modelling...
sealed trait FqnSymbol {
  def fqn: String
  def line: Option[Int]
  def source: Option[String] // uri
  def declAs: DeclaredAs
  def access: Access
  def scalaName: Option[String]

  def sourceFileObject(implicit vfs: EnsimeVFS): Option[FileObject] = source.map(vfs.vfile)
  def toSearchResult: String = s"$declAs ${scalaName.getOrElse(fqn)}"
}

object FqnSymbol {
  private[graph] def fromFullyQualifiedReference(ref: FullyQualifiedReference): Option[FqnSymbol] = ref.fqn match {
    case cn: ClassName if !cn.isPrimitive => Some(ClassDef(ref.fqn.fqnString, null, null, None, None, null, None, None, None))
    case fn: FieldName => Some(Field(ref.fqn.fqnString, None, None, None, null, None))
    case mn: MethodName => Some(Method(ref.fqn.fqnString, None, None, null, None))
    case _ => None
  }
}

sealed trait Hierarchy
object Hierarchy {
  sealed trait Direction
  case object Supertypes extends Direction
  case object Subtypes extends Direction
}

final case class TypeHierarchy(aClass: ClassDef, typeRefs: Seq[Hierarchy]) extends Hierarchy

final case class ClassDef(
    fqn: String,
    file: String, // the underlying file (should be shared)
    path: String, // the VFS handle (e.g. classes in jars)
    source: Option[String], // should be shared
    line: Option[Int],
    access: Access,
    scalaName: Option[String],
    scalapDeclaredAs: Option[DeclaredAs],
    jdi: Option[String] // the JDI name for the source: bin/pkg/Source.scala (should be shared)
) extends FqnSymbol with Hierarchy {
  override def declAs: DeclaredAs = scalapDeclaredAs.getOrElse(DeclaredAs.Class)
}

sealed trait Member extends FqnSymbol

final case class Field(
    fqn: String,
    internal: Option[String],
    line: Option[Int],
    source: Option[String],
    access: Access,
    scalaName: Option[String]
) extends Member {
  override def declAs: DeclaredAs = DeclaredAs.Field
}

final case class Method(
    fqn: String,
    line: Option[Int],
    source: Option[String],
    access: Access,
    scalaName: Option[String]
) extends Member {

  override def declAs: DeclaredAs = DeclaredAs.Method
}

final case class UsageLocation(file: Option[String], line: Option[Int])

final case class FileCheck(filename: String, timestamp: Timestamp) {
  def file(implicit vfs: EnsimeVFS): FileObject = vfs.vfile(filename)
  def lastModified: Long = timestamp.getTime
  def changed(implicit vfs: EnsimeVFS): Boolean = file.getContent.getLastModifiedTime != lastModified
}
object FileCheck extends ((String, Timestamp) => FileCheck) {
  def apply(f: FileObject): FileCheck = {
    val name = f.uriString
    val ts = if (f.exists()) new Timestamp(f.getContent.getLastModifiedTime)
    else new Timestamp(-1L)
    FileCheck(name, ts)
  }

  def fromPath(path: String)(implicit vfs: EnsimeVFS): FileCheck = apply(vfs.vfile(path))
}

case class Job(name: String, items: Int, queuedTime: Option[Long] = None, startedTime: Option[Long] = None) {
  def desc: String = s"$name:$items"

  def sinceQueued(): Option[Long] = queuedTime.map(System.currentTimeMillis() - _)
  def sinceStarted(): Option[Long] = startedTime.map(System.currentTimeMillis() - _)

  def queued(): Job = copy(queuedTime = Some(System.currentTimeMillis()))
  def started(): Job = copy(startedTime = Some(System.currentTimeMillis()))
}

object Job {
  val current = new scala.util.DynamicVariable[Option[Job]](None)
  def withCurrent[A](name: String, items: Int)(f: => A): A = current.withValue(Some(Job(name, items)))(f)
}

// core/it:test-only *Search* -- -z prestine
class GraphService(dir: File) extends SLF4JLogging {
  import org.ensime.indexer.graph.GraphService._

  lazy val csvWriter = {
    val w = new java.io.PrintWriter(dir.getParent + "/graph-profile.csv")
    w.println("Job,Items,Wait time,Run time,Total time,Remaining in queue")
    w
  }

  // all methods return Future, which means we can do isolation by
  // doing all work on a single worker Thread. We can't optimise until
  // we better understand the concurrency limitations (the fact that
  // we have to write dummy vertices to add edges doesn't help)
  private val pools = 1
  private val executor = new java.util.concurrent.ThreadPoolExecutor(
    1, 1, 0L, TimeUnit.MILLISECONDS, new java.util.concurrent.LinkedBlockingQueue[Runnable](),
    new ThreadFactory() {
      override def newThread(runnable: Runnable): Thread = {
        val thread = Executors.defaultThreadFactory().newThread(runnable)
        thread.setName("GraphService")
        thread.setDaemon(true)
        thread
      }
    }
  ) {
    val jobs = scala.collection.concurrent.TrieMap[Runnable, Job]()

    override def execute(r: Runnable) = {
      Job.current.value.fold(log.error(s"No current job set while queuing $r", new Exception)) { job =>
        log.debug(s"Queueing: $r: ${job.desc} (queue size ${getQueue.size})")
        jobs.put(r, job.queued())
      }
      super.execute(r)
    }

    override def beforeExecute(t: Thread, r: Runnable) = {
      for {
        job <- jobs.get(r)
        waitTime <- job.sinceQueued()
      } yield {
        log.debug(s"Starting: $r: ${job.desc} (waited $waitTime, queue size ${getQueue.size})")
        jobs.put(r, job.started())
      }.getOrElse(log.error(s"Failed to record start time for job $r (${jobs.get(r)})", new Exception))

      super.beforeExecute(t, r)
    }

    override def afterExecute(r: Runnable, t: Throwable) = {
      for {
        job <- jobs.get(r)
        totalTime <- job.sinceQueued()
        runTime <- job.sinceStarted()
      } yield {
        val waitTime = totalTime - runTime
        val queueSize = getQueue.size
        log.debug(s"Finished: $r: ${job.desc} (waited $waitTime, took $runTime, total $totalTime, queue size: $queueSize)")
        if (runTime > 1000) log.warn(s"Slow query: $r: ${job.desc} (took $runTime)")
        csvWriter.println(s"${job.name},${job.items},$waitTime,$runTime,$totalTime,$queueSize")
        csvWriter.flush()
        jobs.remove(r)
      }.getOrElse(log.error(s"Failed to record start time for job $r (${jobs.get(r)})", new Exception))

      super.afterExecute(r, t)
    }

    def getTimeElapsed(r: Runnable, m: scala.collection.concurrent.TrieMap[Runnable, Long]) =
      m.get(r).fold("NOT AVAILABLE")(t => s"${System.currentTimeMillis() - t} ms")
  }

  private implicit val ec = ExecutionContext.fromExecutor(executor)

  private implicit lazy val db: OrientGraphFactory = {
    // http://orientdb.com/docs/2.1/Performance-Tuning.html

    OGlobalConfiguration.USE_WAL.setValue(true)
    OGlobalConfiguration.DISK_CACHE_SIZE.setValue(64) // 64MB is far more sensible than 4GB

    //This is a hack, that resolves some classloading issues in OrientDB.
    //https://github.com/orientechnologies/orientdb/issues/5146
    if (ODatabaseRecordThreadLocal.INSTANCE == null) {
      sys.error("Calling this manually prevents an initialization issue.")
    }
    Orient.setRegisterDatabaseByPath(true)

    val url = "plocal:" + dir.getAbsolutePath
    val db = new OrientGraphFactory(url).setupPool(pools, pools)
    db.setAutoStartTx(false)
    db.setUseLightweightEdges(true)
    db.setUseLog(true)

    db
  }

  def shutdown(): Future[Unit] = Future {
    blocking {
      try {
        executor.shutdownNow()
        executor.awaitTermination(30, TimeUnit.SECONDS)
        ()
      } finally {
        Try(db.close())
        Try(db.getDatabase.getStorage.close(true, false))
      }
    }
  }(ExecutionContext.Implicits.global) // must be from a different thread than the executor

  if (!dir.exists) {
    log.info("creating the graph database...")
    dir.mkdirs()

    // schema changes are not transactional
    val g = db.getNoTx()
    val fqnSymbolClass = g.createVertexType("FqnSymbol")
    fqnSymbolClass.createProperty("fqn", OType.STRING).setMandatory(true)
    fqnSymbolClass.createProperty("line", OType.INTEGER).setMandatory(false)
    fqnSymbolClass.createProperty("source", OType.STRING).setMandatory(false)
    val memberSymbolClass = g.createVertexType("Member", fqnSymbolClass)

    g.createVertexFrom[ClassDef](superClass = Some(fqnSymbolClass))
    g.createVertexFrom[Method](superClass = Some(memberSymbolClass))
    g.createVertexFrom[Field](superClass = Some(memberSymbolClass))
    g.createVertexFrom[FileCheck]()

    g.createEdge[DefinedIn.type]
      .createEdge[EnclosingClass.type]
      .createEdge[UsedIn.type]
      .createEdge[IsParent.type]
    g.createIndexOn[Vertex, FqnSymbol, String](Unique)
    g.createIndexOn[Vertex, FileCheck, String](Unique)

    g.shutdown()

    log.info("... created the graph database")
  }

  def knownFiles(): Future[Seq[FileCheck]] = withGraphAsync("knownFiles") { implicit g =>
    RichGraph.allV[FileCheck]
  }

  def outOfDate(f: FileObject)(implicit vfs: EnsimeVFS): Future[Boolean] = withGraphAsync("outOfDate") { implicit g =>
    RichGraph.readUniqueV[FileCheck, String](f.uriString) match {
      case None => true
      case Some(v) => v.toDomain.changed
    }
  }

  def persist(symbols: Seq[SourceSymbolInfo]): Future[Int] = withGraphAsync("persist", symbols.size) { implicit g =>
    val checks = mutable.Map.empty[String, VertexT[FileCheck]]
    val classes = mutable.Map.empty[String, VertexT[ClassDef]]

    g.begin()
    symbols.foreach { s =>
      val scalaName = s.scalapSymbol.map(_.scalaName)
      val typeSignature = s.scalapSymbol.map(_.typeSignature)
      val declAs = s.scalapSymbol.map(_.declaredAs)
      val vertex = s match {
        case EmptySourceSymbolInfo(fileCheck) =>
          if (!checks.contains(fileCheck.filename)) {
            RichGraph.upsertV[FileCheck, String](fileCheck)
          }
          None

        case ClassSymbolInfo(fileCheck, path, source, refs, bs, scalap, jdi) =>
          val classDef = ClassDef(bs.fqn, fileCheck.filename, path, source, bs.source.line, bs.access, scalaName, declAs, jdi)

          val fileV = checks.getOrElse(fileCheck.filename, RichGraph.upsertV[FileCheck, String](fileCheck))
          val classV = RichGraph.upsertV[ClassDef, String](classDef)
          classes += (bs.fqn -> classV)
          RichGraph.insertE(classV, fileV, DefinedIn)
          val superClass = bs.superClass.map(
            name => ClassDef(name.fqnString, null, null, None, None, null, None, None, None)
          )
          val interfaces = bs.interfaces.map(
            name => ClassDef(name.fqnString, null, null, None, None, null, None, None, None)
          )
          (superClass.toList ::: interfaces).foreach { cdef =>
            val parentV = RichGraph.insertIfNotExists[ClassDef, String](cdef)
            RichGraph.insertE(classV, parentV, IsParent)
          }
          Some(classV)

        case MethodSymbolInfo(_, source, refs, bs, scalap) =>
          val owner = classes(bs.name.owner.fqnString)
          val method = Method(s.fqn, bs.line, source, bs.access, (scalaName ++ typeSignature).reduceOption(_ + _))
          val methodV: VertexT[FqnSymbol] = RichGraph.upsertV[Method, String](method)
          RichGraph.insertE(methodV, owner, EnclosingClass)
          Some(methodV)

        case FieldSymbolInfo(_, source, refs, bs, scalap) =>
          val owner = classes(bs.name.owner.fqnString)
          val field = Field(bs.name.fqnString, Some(s.fqn), None, source, bs.access, scalaName)
          val fieldV: VertexT[FqnSymbol] = RichGraph.upsertV[Field, String](field)
          RichGraph.insertE(fieldV, owner, EnclosingClass)
          Some(fieldV)

        case TypeAliasSymbolInfo(_, source, t) =>
          val owner = classes(t.owner.fqnString)
          val field = Field(s.fqn, None, None, source, t.access, Some(t.scalaName + t.typeSignature))
          val fieldV: VertexT[FqnSymbol] = RichGraph.upsertV[Field, String](field)
          RichGraph.insertE(fieldV, owner, EnclosingClass)
          Some(fieldV)
      }
      s.internalRefs.foreach { ref =>
        val sym = FqnSymbol.fromFullyQualifiedReference(ref)
        val usage: Option[VertexT[FqnSymbol]] = sym.map {
          case cd: ClassDef => RichGraph.insertIfNotExists[ClassDef, String](cd)
          case m: Method => RichGraph.insertIfNotExists[Method, String](m)
          case f: Field => RichGraph.insertIfNotExists[Field, String](f)
        }
        for {
          u <- usage
          v <- vertex
        } yield {
          val intermediary: VertexT[UsageLocation] = RichGraph.insertV[UsageLocation](UsageLocation(v.toDomain.source, ref.line))
          RichGraph.insertE(u, intermediary, UsedAt)
          RichGraph.insertE(intermediary, v, UsedIn)
        }
      }
    }

    symbols.collect {
      case c: ClassSymbolInfo => c.bytecodeSymbol.innerClasses.foreach { inner =>
        for {
          innerClassV: VertexT[FqnSymbol] <- classes.get(inner.fqnString)
          outerClassV <- classes.get(c.fqn)
        } yield {
          RichGraph.insertE(innerClassV, outerClassV, EnclosingClass)
          classes.get(s"${c.fqn}$$").foreach(RichGraph.insertE(innerClassV, _, EnclosingClass))
        }
      }
    }
    g.commit()

    symbols.size
  }

  /**
   * Removes given `files` from the graph.
   */
  def removeFiles(files: List[FileObject]): Future[Int] = withGraphAsync("removeFiles", files.size) { implicit g =>
    RichGraph.removeV(files.map(FileCheck(_)))
  }

  /**
   * Finds the FqnSymbol uniquely identified by `fqn`.
   */
  def find(fqn: String): Future[Option[FqnSymbol]] = withGraphAsync("findSingle") { implicit g =>
    RichGraph.readUniqueV[FqnSymbol, String](fqn).map(_.toDomain)
  }

  /**
   * Finds all FqnSymbol's identified by unique `fqns`.
   */
  def find(fqns: List[FqnIndex]): Future[List[FqnSymbol]] = withGraphAsync("findMultiple", fqns.size) { implicit g =>
    fqns.flatMap(fqn =>
      RichGraph.readUniqueV[FqnSymbol, String](fqn.fqn).map(_.toDomain))
  }

  def getClassHierarchy(fqn: String, hierarchyType: Hierarchy.Direction, levels: Option[Int]): Future[Option[Hierarchy]] = withGraphAsync("getClassHierarchy") { implicit g =>
    RichGraph.classHierarchy[String](fqn, hierarchyType, levels)
  }

  def findUsageLocations(fqn: String): Future[Iterable[UsageLocation]] = withGraphAsync("findUsageLocations") { implicit g =>
    RichGraph.findUsageLocations[String](fqn).map(_.toDomain).distinct
  }

  def findUsages(fqn: String): Future[Iterable[FqnSymbol]] = withGraphAsync("findUsages") { implicit g =>
    RichGraph.findUsages[String](fqn).map(_.toDomain)
  }

  def findClasses(source: EnsimeFile): Future[Seq[ClassDef]] = withGraphAsync("findClassesFile") { implicit g =>
    val uri = Some(source.uriString)
    RichGraph.findV[ClassDef]("jdi") { c => c.source == uri }
  }

  def findClasses(jdi: String): Future[Seq[ClassDef]] = withGraphAsync("findClassesJdi") { implicit g =>
    RichGraph.findV[ClassDef]("source") { c => c.jdi == Some(jdi) }
  }

}

object GraphService {
  private[indexer] case object DefinedIn extends EdgeT[ClassDef, FileCheck]
  private[indexer] case object EnclosingClass extends EdgeT[FqnSymbol, ClassDef]
  private[indexer] case object UsedAt extends EdgeT[FqnSymbol, UsageLocation]
  private[indexer] case object UsedIn extends EdgeT[UsageLocation, FqnSymbol]
  private[indexer] case object IsParent extends EdgeT[ClassDef, ClassDef]

  // the domain-specific formats for schema generation
  import org.ensime.indexer.orientdb.schema.impl._
  import org.ensime.util.stringymap.api._
  import org.ensime.util.stringymap.impl._

  implicit object AccessSPrimitive extends SPrimitive[Access] {
    import org.objectweb.asm.Opcodes._
    import SPrimitive.IntSPrimitive

    def toValue(v: Access): java.lang.Integer =
      if (v == null) null
      else {
        val code = v match {
          case Public => ACC_PUBLIC
          case Private => ACC_PRIVATE
          case Protected => ACC_PROTECTED
          case Default => 0
        }
        IntSPrimitive.toValue(code)
      }

    def fromValue(v: AnyRef): Either[String, Access] = IntSPrimitive.fromValue(v).right.map(Access(_))
  }

  implicit object DeclaredAsSPrimitive extends SPrimitive[DeclaredAs] {
    import org.ensime.util.enums._
    import SPrimitive.StringSPrimitive
    private val lookup: Map[String, DeclaredAs] = implicitly[AdtToMap[DeclaredAs]].lookup
    def toValue(v: DeclaredAs): java.lang.String = if (v == null) null else StringSPrimitive.toValue(v.toString)
    def fromValue(v: AnyRef): Either[String, DeclaredAs] = StringSPrimitive.fromValue(v).right.map(lookup)
  }

  implicit val FileCheckBdf: BigDataFormat[FileCheck] = cachedImplicit
  implicit val FileCheckS: SchemaFormat[FileCheck] = cachedImplicit

  implicit val ClassDefBdf: BigDataFormat[ClassDef] = cachedImplicit
  implicit val ClassDefS: SchemaFormat[ClassDef] = cachedImplicit

  implicit val MethodBdf: BigDataFormat[Method] = cachedImplicit
  implicit val MethodS: SchemaFormat[Method] = cachedImplicit

  implicit val FieldBdf: BigDataFormat[Field] = cachedImplicit
  implicit val FieldS: SchemaFormat[Field] = cachedImplicit

  implicit val FqnSymbolBdf: BigDataFormat[FqnSymbol] = cachedImplicit

  implicit val LineNumberBdf: BigDataFormat[UsageLocation] = cachedImplicit

  implicit val DefinedInS: BigDataFormat[DefinedIn.type] = cachedImplicit
  implicit val EnclosingClassS: BigDataFormat[EnclosingClass.type] = cachedImplicit
  implicit val UsedInS: BigDataFormat[UsedIn.type] = cachedImplicit
  implicit val UsedAtS: BigDataFormat[UsedAt.type] = cachedImplicit
  implicit val IsParentS: BigDataFormat[IsParent.type] = cachedImplicit

  implicit val UniqueFileCheckV: OrientIdFormat[FileCheck, String] =
    new OrientIdFormat[FileCheck, String] {
      override def key = "filename"
      override def value(t: FileCheck): String = t.filename
    }

  implicit val FqnIndexV: OrientIdFormat[FqnIndex, String] =
    new OrientIdFormat[FqnIndex, String] {
      override def key = "fqn"
      override def value(t: FqnIndex): String = t.fqn
    }

  class UniqueFqnSymbol[T <: FqnSymbol] extends OrientIdFormat[T, String] {
    override def key = "fqn"
    override def value(t: T): String = t.fqn
  }

  implicit val UniqueClassDefV: UniqueFqnSymbol[ClassDef] = new UniqueFqnSymbol[ClassDef]
  implicit val UniqueMethodV: UniqueFqnSymbol[Method] = new UniqueFqnSymbol[Method]
  implicit val UniqueFieldV: UniqueFqnSymbol[Field] = new UniqueFqnSymbol[Field]
  implicit val UniqueFqnSymbolV: UniqueFqnSymbol[FqnSymbol] = new UniqueFqnSymbol[FqnSymbol]

}
