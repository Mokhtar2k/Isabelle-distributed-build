package isabelle.distributedbuild

import isabelle.Sessions.{Selection, load_structure}
import isabelle._

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.Random
import scala.util.control.Breaks._


object Distributed_Build {
  val isabelle_home: String = Isabelle_System.getenv("ISABELLE_HOME")
  val isabelle_home_user: String = Isabelle_System.getenv("ISABELLE_HOME_USER")
  val home: String = Isabelle_System.getenv("HOME")
  val begin_from: String = DateTimeFormatter.ofPattern("MM.dd.yy-HH:mm").format(LocalDateTime.now())


  class State private (
    var pending_jobs: List[String] = Nil,
    var running_jobs: List[String] = Nil,
    var terminated_jobs: List[String] = Nil,
    var failed_jobs: List[String] = Nil,
    var process_results_futures: List[Running_Future] = Nil,
    var presentation_sessions: List[String] = Nil,
    var leaf_sessions: List[String] = Nil,
    var worker_nodes: List[String] = Nil,
    var partitions: List[String] = Nil,
    var time_graph: Time_Graph = Time_Graph.empty,
    private var graph: Graph[String, Sessions.Info] = Graph.empty) {


    def generate_leaf_sessions(build_all_heaps: Boolean): Unit = {
      if(!build_all_heaps) {
        var non_leafs: Set[String] = Set.empty
        for(node <- graph.iterator) {
          non_leafs = non_leafs union node._2._2._1
        }
        leaf_sessions = pending_jobs diff non_leafs.toList
      }
    }

    def del_node(session_name: String): Unit = {
      graph = graph.del_node(session_name)
    }

    def sessions_without_parents(): List[String] = synchronized {
      for {
        node <- graph.iterator.toList
        if node._2._2._1.isEmpty && pending_jobs.contains(node._1)
        } yield node._1
      }

    def is_empty: Boolean = synchronized( {
      graph.is_empty
    })

    def remove_failed(failed_session: String, progress: Progress): Unit = synchronized {
      var failed_list: List[String] = Nil
      failed_list ::= failed_session
      failed_jobs ::= failed_session
      var failed_session_found = true
      while(failed_session_found) {
        failed_session_found = false
        for (node <- graph.iterator) {
          val failed_parent_sessions = node._2._2._1.toList.filter(session => failed_list.contains(session))
          if (failed_parent_sessions.nonEmpty && !failed_list.contains(node._1)) {
            failed_session_found = true
            failed_jobs ::= node._1
            failed_list ::= node._1
          }
        }
      }
      for(session_name <- failed_list) {
        progress.echo(s"$session_name building is canceled")
        graph = graph.del_node(session_name)
      }
      pending_jobs = pending_jobs diff failed_list
    }
  }

  object State {
    def apply(graph: Graph[String, Sessions.Info], pending_jobs: List[String]): State = {
      new State(pending_jobs = pending_jobs, graph = graph)
    }
  }



  class Database(db: SQL.Database) {
    val session_name: SQL.Column = SQL.Column.string("session_name").make_primary_key
    val session_threads: SQL.Column = SQL.Column.int("threads")
    val session_time: SQL.Column = SQL.Column.string("elapsed")
    val build_recommendations = List(session_name, session_threads, session_time)
    val table: SQL.Table = SQL.Table("sessions", build_recommendations)

    def insert_database(session_name: String, thread: Int, time: String): Unit = {
      db.transaction {
        db.using_statement(table.insert()) { stmt =>
          stmt.string(1) = session_name
          stmt.int(2) = thread
          stmt.string(3) = time
          stmt.execute()
        }
      }
    }

    def change_thread_time(session_name: String, thread: Int, time: String): Unit = {
      db.using_statement(table.delete(s"WHERE session_name = '$session_name'")) {
        stmt =>
          stmt.execute()
      }
      insert_database(session_name, thread, time)
    }


    def get_threads(name_session: String): Int = {
      db.using_statement(table.select(List(session_threads), session_name.where_equal(name_session))) {
        stmt =>
          val res = stmt.execute_query()
          if (!res.next()) -1 else res.int(session_threads)
      }
    }

    def get_time(name_session: String): String = {
      db.using_statement(table.select(List(session_time), session_name.where_equal(name_session))) {
        stmt =>
          val res = stmt.execute_query()
          if (!res.next()) "" else res.string(session_time)
      }
    }

  }

  def time_string_to_long(time: String): Long = {
    if(time.equals("NaN")) {
      return -1
    }
    val time_array = time.split(":")
    time_array(0).toLong * 3600 + time_array(1).toLong * 60 + time_array(2).toLong
  }

  sealed case class Node_Time(node: Node, time: String)

  class Node(name: String) {
    var next_nodes: List[Node_Time] = Nil
    val node_name: String = name

    def add_node(node: Node, time: String): Unit = {
      next_nodes ::= Node_Time(node, time)
    }

    def get_node(name: String): Node = {
      if(next_nodes.isEmpty)
        return null
      val found_node = next_nodes.filter(node_time => node_time.node.node_name.equals(name))

      if(found_node.isEmpty) {
        var results: List[Node] = for(node_time <- next_nodes)
          yield node_time.node.get_node(name)
        results = results.filter(node => node != null)
        if(results.isEmpty)
          return null
        return results.head
      }

      found_node.head.node
    }



    def path_time: List[(Long, List[String])] = {
      if(next_nodes.isEmpty)
        return List((0, List(node_name)))

      val list_pathes = (for(node_time <- next_nodes)
        yield  {
          val list_paths = node_time.node.path_time
          for(sub_path <- list_paths)
            yield ((sub_path._1 + time_string_to_long(node_time.time)), node_time.node.node_name :: sub_path._2)
        })

      list_pathes.flatten
    }
  }

  class Time_Graph {
    val root: Node = new Node("root")

    def get_node(name: String): Node = {
      root.get_node(name)
    }



    def all_paths: List[(Long, List[String])] = {
      root.path_time.map(tuple => (tuple._1, tuple._2.dropRight(1)))
    }
  }

  object Time_Graph {
    def empty: Time_Graph = {
      new Time_Graph()
    }
  }

  sealed case class Running_Future(future: Future[Process_Result],
                                   session_name: String,
                                   thread: Int,
                                   policy_applied: Boolean,
                                   fresh_record:Boolean,
                                   built_heap: Boolean)



  def distributed_build(
    database: Database,
    predict_build: Boolean,
    policy_alpha: Double,
    parallelization_strategy: Boolean = false,
    build_name: String,
    options: Options,
    selection: Sessions.Selection = Sessions.Selection.empty,
    progress: Progress = new Progress,
    presentation: Presentation.Context = Presentation.Context.none,
    presentation_triggered: Boolean = false,
    build_all_heaps: Boolean = false,
    clean_build: Boolean = false,
    dirs: List[Path] = Nil,
    select_dirs: List[Path] = Nil,
    check_keywords: Set[String] = Set.empty,
    list_files: Boolean = false,
    infos: List[Sessions.Info] = Nil,
    verbose: Boolean = false,
  ): State = {

    val store = Sessions.store(options)

    val full_sessions =
      Sessions.load_structure(options, dirs = dirs, select_dirs = select_dirs, infos = infos)

    val full_sessions_selection = full_sessions.imports_selection(selection)
    val full_sessions_selected = full_sessions_selection.toSet

    val sessions_structure =
      load_structure(options, dirs = dirs, select_dirs = select_dirs).selection(selection)

    val state = State(pending_jobs = sessions_structure.build_topological_order,
      graph = sessions_structure.build_graph)
    state.generate_leaf_sessions(build_all_heaps)


    if(!presentation.equals(Presentation.Context.none)) {
      val deps =
        Sessions.deps(full_sessions.selection(selection),
          progress = progress, inlined_files = true, verbose = verbose,
          list_files = list_files, check_keywords = check_keywords).check_errors

      state.presentation_sessions =
        (for {
          session_name <- deps.sessions_structure.build_topological_order.iterator
          info <- deps.sessions_structure.get(session_name)
          if full_sessions_selected(session_name) && presentation.enabled(info)}
        yield info).toList.map(session_info => session_info.name)

      val presentation_dir = presentation.dir(store)
      Isabelle_System.make_directory(presentation_dir)
      HTML.init_fonts(presentation_dir)
      Isabelle_System.copy_file(Path.explode("~~/lib/logo/isabelle.gif"),
        presentation_dir + Path.explode("isabelle.gif"))
      val title = "The " + XML.text(Isabelle_System.isabelle_name()) + " Library"
      File.write(presentation_dir + Path.explode("index.html"),
        HTML.header +
          """
<head>
  """ + HTML.head_meta + """
  <title>""" + title + """</title>
</head>

<body text="#000000" bgcolor="#FFFFFF" link="#0000FF" vlink="#000099" alink="#404040">
  <center>
    <table width="100%" border="0" cellspacing="10" cellpadding="0">
      <tr>
        <td width="20%" valign="middle" align="center"><a href="https://isabelle.in.tum.de/"><img align="bottom" src="isabelle.gif" width="100" height="86" alt="[Isabelle]" border="0" /></a></td>

        <td width="80%" valign="middle" align="center">
          <table width="90%" border="0" cellspacing="0" cellpadding="20">
            <tr>
              <td valign="middle" align="center" bgcolor="#AACCCC"><font face="Helvetica,Arial" size="+2">""" + title + """</font></td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </center>
  <hr />
""" + File.read(Path.explode("~~/lib/html/library_index_content.template")) +
          """
</body>
""" + HTML.footer)
    }


    progress.echo(s"${state.pending_jobs.length} sessions will be built")

    def checkJobs(): Unit = {

      while(state.pending_jobs.nonEmpty || state.running_jobs.nonEmpty) {
        val process = Isabelle_System.bash("sudo sacct --format=\"JobName%-100,State\" --starttime " + begin_from)
        val sessions_info = process.out_lines.map(str => str.trim.replaceAll(" +", " ").split(" "))
        val dead_jobs = sessions_info.filter(session_info => !session_info(1).equals("RUNNING")
          && !session_info(1).equals("PENDING") && state.running_jobs.contains(session_info(0)))
        for(finished_session_info <- dead_jobs) {
          finished_session_info(1) match {
            case "COMPLETED" =>
              state.terminated_jobs ::= finished_session_info(0)
              state.del_node(finished_session_info(0).dropRight(20))
              if(verbose) {
                val terminated_future: Running_Future = state.process_results_futures
                  .filter(running_future => running_future.session_name.equals(finished_session_info(0).dropRight(20))).head
                val factor_line = terminated_future.future.get_finished.out_lines
                  .filter(line => line.startsWith(s"Finished ${terminated_future.session_name} ("))
                if(factor_line.nonEmpty) {
                  progress.echo(factor_line.head)
                }
                else
                  progress.echo("Finished " + finished_session_info(0)dropRight(20))
              }
              else {
                progress.echo("Finished " + finished_session_info(0)dropRight(20))
              }
            case _ => state.remove_failed(finished_session_info(0).dropRight(20), progress)
              val failed_future: Running_Future = state.process_results_futures
                .filter(running_future => running_future.session_name.equals(finished_session_info(0).dropRight(20))).head
              failed_future.future.get_finished.print_stdout
          }
        }
        val dead_jobs_names = dead_jobs.map(session_info => session_info(0))
        state.running_jobs = state.running_jobs.filter(session_name => !dead_jobs_names.contains(session_name))
        Thread.sleep(1000)
      }
      progress.echo(s"${state.terminated_jobs.length} were built successfully")
      progress.echo(s"${state.failed_jobs.length} failed to be built")
    }
    var original_graph = sessions_structure.build_graph
    val graph_list = original_graph.iterator.toList
    while(!original_graph.is_empty){
      val sessions_to_add: List[String] = for {
        node <- original_graph.iterator.toList
        if (node._2._2._1.isEmpty && state.pending_jobs.contains(node._1))
      } yield node._1
      for(session_name <- sessions_to_add) {
        val session_node = graph_list.filter(node => node._1.equals(session_name)).head
        val parents = session_node._2._2._1.toList
        var nodes: List[Node] = Nil
        if(parents.isEmpty) {
          nodes ::= state.time_graph.root
        }
        else {
          nodes = (for (parent <- parents)
            yield state.time_graph.get_node(parent)).filter(node => node != null)
        }
        val time = database.get_time(session_name)
        for(node <- nodes) {
          node.add_node(new Node(session_name), time)
        }
        original_graph = original_graph.del_node(session_name)
      }
    }

    if(predict_build) {
      val all_paths = state.time_graph.all_paths.filter(tuple => tuple._1 >= 1500)
      val total_time = all_paths.map(tuple => tuple._1).max
      val longest_path = all_paths.find(tuple => tuple._1 == total_time).get
      val hours = total_time / 3600
      val minutes = (total_time % 3600) / 60
      val seconds = total_time - hours * 3600 - minutes * 60

      progress.echo(s"This build will theoretically take $hours hours $minutes minutes $seconds seconds")
      progress.echo("With the following path " + longest_path._2.toString())

      sys.exit(0)
    }

    if (clean_build) {
      for (name <- full_sessions.imports_descendants(full_sessions_selection)) {
        val (relevant, ok) = store.clean_output(name)
        if (relevant) {
          if (ok) progress.echo("Cleaned " + name)
          else progress.echo(name + " FAILED to clean")
        }
      }
    }

    val info_process = Isabelle_System.bash("sudo sinfo --all --Node --format=\"%N %.6D %P %.11T %.4c %C %e\"")
    val nodes_info = info_process.out_lines.map(str => str.trim.replaceAll(" +", " ").split(" "))
    val max_CPU: Int = nodes_info.drop(1).map(line => line(4).toInt).max

    state.worker_nodes = nodes_info.drop(1).map(line => line(0))
    state.partitions = nodes_info.drop(1).map(line => line(2).replace("*","")).distinct

    class Strategy {

      def default_strategy() : Unit = {
        while (! state.is_empty) {
          val sessions_to_build: List[String] = state.sessions_without_parents()
          for (session_name <- sessions_to_build) {
            val built_heap: Boolean = ! build_all_heaps && state.leaf_sessions.contains(session_name)
            val build_flag =
              if (built_heap)
                ""
              else
                "-b"
            var threads = database.get_threads(session_name)
            val original_threads = threads
            var fresh_record: Boolean = false
            var policy_applied: Boolean = false
            if (threads == - 1) {
              threads = 8
              database.insert_database(session_name = session_name, thread = threads, time = "00:00:00")
              fresh_record = true
            }
            else {
              time_string_to_long (database.get_time(session_name)) match {
                case -1 =>
                  fresh_record = true
                case _ =>
                  fresh_record = false
                  val random: Float = Random.nextFloat()
                  if (random >= 1 - policy_alpha) {
                    threads match {
                      case 1 => threads += Random.between(1, 3)
                      case _ =>
                        if (random > 0.5 ) {
                          threads += Random.between(- 2, 0)
                        }
                        else {
                          threads += Random.between(1, 3)
                        }
                    }
                    policy_applied = true
                  }
              }
            }
            threads = (threads max 1) min max_CPU

            if (verbose) {
              progress.echo(s"Building $session_name with $threads threads")
              if (policy_applied) {
                progress.echo(s"The number of threads before applying the strategy was $original_threads")
              }
            } else {
              progress.echo(s"Building $session_name")
            }

            val partition_flag = time_string_to_long(database.get_time(session_name)) match {
              case x: Long if (x > 360 && state.partitions.contains("isabelle-fast")) => "--partition=isabelle-fast"
              case _ => ""
            }

            val presentation_flag =
              if (state.presentation_sessions.contains(session_name) )
                s"-P $home/browser_info"
              else
                ""
            val future = Future.fork(Isabelle_System.bash(s"srun -N1 -n1 -c$threads " +
              s"$partition_flag --job-name=$session_name-$build_name $isabelle_home/bin/isabelle build -o" +
              s" threads=$threads $build_flag $presentation_flag $session_name", strict = false) )

            state.process_results_futures ::=
              Running_Future (future = future, session_name = session_name, thread = threads,
                policy_applied = policy_applied, fresh_record = fresh_record,
                built_heap = built_heap)
            state.running_jobs ::= session_name + "-" + build_name
          }
          state.pending_jobs = state.pending_jobs diff sessions_to_build
          Thread.sleep(500)
        }
      }

      def parallel_strategy() : Unit = {
        val priority_sessions: List[String] = state.time_graph.all_paths
          .filter(tuple => tuple._1 >= 1500).flatMap(tuple => tuple._2).distinct
        while (! state.is_empty) {
          val sessions_to_build: List[String] = state.sessions_without_parents()
          for (session_name <- sessions_to_build) {
            val built_heap: Boolean = ! build_all_heaps && state.leaf_sessions.contains (session_name)
            val build_flag =
              if (built_heap)
                ""
              else
                "-b"
            var partition_flag = ""
            var threads = time_string_to_long(database.get_time(session_name)) match {
              case x: Long if (x < 20) => 1
              case x: Long if (x >= 20 && x < 60) => 2
              case x: Long if (x >= 60 && x < 180) => 3
              case x: Long if (x >= 180 && x < 240) => 4
              case _ => database.get_threads(session_name)
            }

            if(state.running_jobs.length + sessions_to_build.length < 8) {
              threads = database.get_threads(session_name)
              if(state.partitions.contains("isabelle-fast"))
                partition_flag = "--partition=isabelle-fast"
            }

            val priority: String =
              if (priority_sessions.contains(session_name)) {
                if(state.partitions.contains("isabelle-fast"))
                  partition_flag = "--partition=isabelle-fast"
                "--priority=TOP"
              } else
                "--priority=1"


            val original_threads = database.get_threads(session_name)
            var fresh_record: Boolean = false
            val policy_applied: Boolean = false
            if (threads == - 1) {
              threads = 8
              database.insert_database(session_name = session_name, thread = threads, time = "00:00:00")
              fresh_record = true
            }

            threads = (threads max 1) min max_CPU

            if (verbose) {
              progress.echo(s"Building $session_name with $threads threads")
              if (policy_applied) {
                progress.echo(s"The number of threads before applying the strategy was $original_threads")
              }
            } else {
              progress.echo(s"Building $session_name")
            }

            val presentation_flag =
              if (state.presentation_sessions.contains(session_name)) {
                s"-P $home/browser_info"
              } else
                ""
            val future = Future.fork(Isabelle_System.bash(s"srun -N1 -n1 -c$threads $priority " +
              s"$partition_flag --job-name=$session_name-$build_name $isabelle_home/bin/isabelle build -o" +
              s" threads=$threads $build_flag $presentation_flag $session_name", strict = false) )

            state.process_results_futures ::=
              Running_Future (future = future, session_name = session_name, thread = threads,
                policy_applied = policy_applied, fresh_record = fresh_record,
                built_heap = true)
            state.running_jobs ::= session_name + "-" + build_name
          }
          state.pending_jobs = state.pending_jobs diff sessions_to_build
          Thread.sleep(500)
        }
      }
    }

    if(presentation_triggered) {
      Isabelle_System.bash(s"srun -N${state.worker_nodes.length} sudo rm -R $home/browser_info")
      Isabelle_System.bash(s"srun -N${state.worker_nodes.length} mkdir $home/browser_info")
    }

    val thread = new Thread {
      override def run() : Unit = {
        val strategy = new Strategy()
        if (parallelization_strategy) {
          strategy.parallel_strategy()
        } else {
          strategy.default_strategy()
        }
      }
    }

    thread.start()
    checkJobs()
    thread.join()

    if(presentation_triggered) {
      Isabelle_System.bash(s"srun -N${state.worker_nodes.length} rsync -a $home/browser_info/ $isabelle_home_user/browser_info")
    }

    if(!parallelization_strategy) {
      for (running_future <- state.process_results_futures) {
        breakable {
          val factor_line = running_future.future.get_finished.out_lines
            .filter(line => line.startsWith(s"Finished ${running_future.session_name} ("))
          if (!running_future.future.get_finished.ok || factor_line.isEmpty) {
            break()
          }
          val old_time = database.get_time(running_future.session_name)
          val new_time = factor_line.head.split(' ')(2).drop(1)
          val old_thread = database.get_threads(running_future.session_name)
          val new_thread = running_future.thread
          if (running_future.fresh_record) {
            database.change_thread_time(session_name = running_future.session_name, thread = old_thread,
              time = new_time)
            break()
          }
          if (running_future.policy_applied && (time_string_to_long(new_time) < time_string_to_long(old_time))) {
            database.change_thread_time(session_name = running_future.session_name, thread = new_thread, time = new_time)
          }
          else if (!running_future.policy_applied && (time_string_to_long(new_time) < time_string_to_long(old_time))) {
            database.change_thread_time(session_name = running_future.session_name, thread = old_thread, time = new_time)
          }
        }
      }
    }

    for (running_future <- state.process_results_futures) {
      val build_history = new Database(SQLite.open_database(Path.explode(s"$isabelle_home_user/build_history.db")))

      if(running_future.future.get_finished.ok) {
        breakable {
          val factor_line = running_future.future.get_finished.out_lines
            .filter(line => line.startsWith(s"Finished ${running_future.session_name} ("))
          if (!running_future.future.get_finished.ok || factor_line.isEmpty) {
            break()
          }
          val time = factor_line.head.split(' ')(2).drop(1)
          build_history.insert_database(session_name = running_future.session_name,
            thread = running_future.thread, time = time)
        }
      }
    }

    state

    }


  val isabelle_tool: Isabelle_Tool = Isabelle_Tool("distributed_build", "build and manage Isabelle sessions in a distributed system",
    Scala_Project.here, args => {
      val build_options = Word.explode(Isabelle_System.getenv("ISABELLE_BUILD_OPTIONS"))
      var base_sessions: List[String] = Nil
      var select_dirs: List[Path] = Nil
      var presentation = Presentation.Context.none
      var requirements = false
      var exclude_session_groups: List[String] = Nil
      var all_sessions = false
      var build_all_heaps = false
      var clean_build = false
      var dirs: List[Path] = Nil
      var session_groups: List[String] = Nil
      var options = Options.init(opts = build_options)
      var verbose = false
      var exclude_sessions: List[String] = Nil
      var check_keywords: Set[String] = Set.empty
      var list_files = false
      var database_path: Path = Path.explode(s"$isabelle_home_user/sessions.db")
      var SQLite_database: Boolean = false
      var postgreSQL_database: String = ""
      var build_name: String = ""
      var postgreSQL: Boolean = false
      var policy_alpha: Double = 0.05
      var predict_build: Boolean = false
      val presentation_path: String = s"$isabelle_home_user/browser_info"
      var presentation_triggered: Boolean = false
      var parallelization_strategy: Boolean = false

      val getopts = Getopts(
        """
Usage: isabelle distributed_build [OPTIONS] [SESSIONS ...]

  Options are:
    -B NAME      include session NAME and all descendants
    -D DIR       include session directory and select its sessions
    -P           enable HTML/PDF presentation in directory ISABELLE_HOME_USER
    -R           refer to requirements of selected sessions
    -X NAME      exclude sessions from group NAME and all descendants
    -a           select all sessions
    -b           build all heap images
    -c           clean build
    -d DIR       include session directory
    -g NAME      select session group NAME
    -h DATABASE  path to a SQLite build database file
    -i DATABASE  credentials to connect to a PostgreSQL build database
    -o OPTION    override Isabelle system OPTION (via NAME=VAL or NAME)
    -p ALPHA     override the policy parameter
    -q           calculate the predictable build time
    -u           use the parallelization strategy
    -v           verbose
    -x NAME      exclude session NAME and all descendants

  Build and manage Isabelle sessions in a distributed system, depending on implicit settings:

""" + Library.indent_lines(2, Build_Log.Settings.show()) + "\n",
        "B:" -> (arg => base_sessions = base_sessions ::: List(arg)),
        "D:" -> (arg => select_dirs = select_dirs ::: List(Path.explode(arg))),
        "P" -> (_ => {
          presentation = Presentation.Context.make(presentation_path)
          presentation_triggered = true
        }),
        "R" -> (_ => requirements = true),
        "X:" -> (arg => exclude_session_groups = exclude_session_groups ::: List(arg)),
        "a" -> (_ => all_sessions = true),
        "b" -> (_ => build_all_heaps = true),
        "c" -> (_ => clean_build = true),
        "d:" -> (arg => dirs = dirs ::: List(Path.explode(arg))),
        "g:" -> (arg => session_groups = session_groups ::: List(arg)),
        "h:" -> (arg => {
          database_path = Path.explode(arg)
          SQLite_database = true
        }),
        "i:" -> (arg => {
          postgreSQL_database = arg
          postgreSQL = true
        }),
        "k:" -> (arg => check_keywords = check_keywords + arg),
        "l" -> (_ => list_files = true),
        "o:" -> (arg => options = options + arg),
        "p:" -> (arg => policy_alpha = arg.toDouble),
        "q" -> (_ => predict_build = true),
        "u" -> (_ => parallelization_strategy = true),
        "v" -> (_ => verbose = true),
        "x:" -> (arg => exclude_sessions = exclude_sessions ::: List(arg)))

      val sessions = getopts(args)

      val progress = new Console_Progress(verbose = verbose)
      System.setProperty("isabelle.threads", "1000")
      val start_date = Date.now()

      if(build_name.isEmpty) build_name = start_date.format(Date.Format("yyyy-MM-dd_HH-mm-ss"))

      if (verbose) {
        progress.echo(
          "Started at " + Build_Log.print_date(start_date) +
            " (" + Isabelle_System.getenv("ML_IDENTIFIER") + " on " + Isabelle_System.hostname() +")")
        progress.echo(Build_Log.Settings.show() + "\n")
      }

      if(SQLite_database && postgreSQL) {
        progress.echo("You can't use SQLite and a postgreSQL databases at the same time")
        sys.exit(1)
      }

      if(policy_alpha > 1 || policy_alpha < 0) {
        progress.echo("The policy parameter must not be bigger than 1 or less than 0")
        sys.exit(1)
      }

      val selection =
        Selection(requirements = requirements, all_sessions = all_sessions, base_sessions = base_sessions,
          exclude_session_groups = exclude_session_groups, exclude_sessions = exclude_sessions,
          session_groups = session_groups, sessions = sessions)

      val build_options2 =
        options +
          "completion_limit=0" +
          "editor_tracing_messages=0" +
          ("pide_reports=" + options.bool("build_pide_reports"))

      val database = new Database(
        if (postgreSQL) {
          val connection_details: Array[String] = postgreSQL_database.split(":")
          PostgreSQL.open_database(connection_details(0), connection_details(1), connection_details(2),
            connection_details(3), connection_details(4).toInt)
        } else
          SQLite.open_database(database_path))

      val state = distributed_build(
        database = database,
        predict_build = predict_build,
        policy_alpha = policy_alpha,
        parallelization_strategy = parallelization_strategy,
        build_name = build_name,
        options = build_options2,
        selection = selection,
        progress = progress,
        presentation = presentation,
        presentation_triggered = presentation_triggered,
        build_all_heaps = build_all_heaps,
        clean_build = clean_build,
        dirs = dirs,
        select_dirs = select_dirs,
        check_keywords = check_keywords,
        list_files = list_files,
        verbose = verbose,
      )

      val end_date = Date.now()
      val elapsed_time = end_date.time - start_date.time

      val total_timing =
        state.process_results_futures.map(running_future => running_future.future.get_finished.timing).foldLeft(Timing.zero)(_ + _).
          copy(elapsed = elapsed_time)

      progress.echo(total_timing.message_resources)

    })
}