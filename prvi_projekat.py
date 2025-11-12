from threading import Lock, Thread, Condition, Event
from queue import Queue, Empty
import time
import json
import subprocess as sp
import multiprocessing as mp

globalGraph = None
rafThreadPool = None
activePlaners = []
planersLock = Lock()
plannerCondition = Condition()


class Node:
    def __init__(self, id: str, deps, action, outputs, resources: dict):
        self.id = id
        self.deps = deps
        self.action = action
        self.outputs = outputs
        self.resources = resources
        self._state = "PENDING"
        self._last_error = None
        self._num_deps = len(deps)
        self.lock = Lock()

    def set_state(self, state):
        with self.lock:
            self._state = state

    def get_state(self):
        with self.lock:
            return self._state

    def set_error(self, error):
        with self.lock:
            self._last_error = error

    def decrement_deps(self):
        # smanjuje broj zavisnosti
        with self.lock:
            if self._num_deps > 0:
                self._num_deps -= 1
            if self._num_deps == 0 and self._state == "PENDING":
                self._state = "READY"

    def reset(self):
        with self.lock:
            self._state = "PENDING"
            self._last_error = None
            self._num_deps = len(self.deps)

    def describe(self):
        with self.lock:
            output = f"DESCRIPTION OF NODE: {self.id}\n"
            output += f"STATE: {self._state}\n"
            output += f"DEPENDENCIES: {", ".join(self.deps)}\n"
            output += f"I/Os: {", ".join(self.outputs)}\n"
            output += f"CPU usage: {self.resources["CPU"]}\n"
            output += f"RAM usage: {self.resources["RAM"]}\n"
            output += f"Number of DEPS: {self._num_deps}\n"

            if self._last_error is not None:
                output += f"Last error: {self._last_error}\n"

            return output


class Graph:
    def __init__(self, capacity: dict):
        self.nodes = {}
        self.capacity = capacity
        self.used_capacity = {"CPU": 0, "RAM": 0}
        self.lock = Lock()

    def add_node(self, node: Node):
        self.nodes[node.id] = node

    def reset_states(self):
        with self.lock:
            for node in self.nodes.values():
                node.reset()
            self.used_capacity = {"CPU": 0, "RAM": 0}

    def acquire_resources(self, resources: dict):
        with self.lock:
            cpu_needed = resources.get("CPU", 0)
            ram_needed = resources.get("RAM", 0)

            cpu = self.used_capacity["CPU"] + cpu_needed <= self.capacity["CPU"]
            ram = self.used_capacity["RAM"] + ram_needed <= self.capacity["RAM"]

            if cpu and ram:
                self.used_capacity["CPU"] += cpu_needed
                self.used_capacity["RAM"] += ram_needed
                return True

            return False

    def release_resources(self, resources: dict):
        with self.lock:
            self.used_capacity["CPU"] = max(
                0, self.used_capacity["CPU"] - resources.get("CPU", 0)
            )
            self.used_capacity["RAM"] = max(
                0, self.used_capacity["RAM"] - resources.get("RAM", 0)
            )

    def describe_node(self, node_id):
        if node_id not in self.nodes:
            return f"Node '{node_id}' does not exist."
        return self.nodes[node_id].describe()

    def node_statistics(self, rafThreadPool):
        with self.lock:
            counts = {"PENDING": 0, "READY": 0, "RUNNING": 0, "DONE": 0, "FAILED": 0}
            for node in self.nodes.values():
                state = node.get_state()
                if state not in counts:
                    counts[state] = 0
                counts[state] += 1
            cpu = self.used_capacity["CPU"]
            ram = self.used_capacity["RAM"]
            return (
                f"Node statistics:\n"
                f"PENDING={counts['PENDING']}, READY={counts['READY']}, RUNNING={counts['RUNNING']}, "
                f"DONE={counts['DONE']}, FAILED={counts['FAILED']}\n"
                f"CPU: {cpu}/{self.capacity['CPU']}, RAM: {ram}/{self.capacity['RAM']}\n"
                f"Active threads: {rafThreadPool.num_active()}\n"
                f"Active planners: {len(activePlaners)}"
            )


class Planer(Thread):
    def __init__(self, target, globalGraph, rafThreadPool, messageQueue):
        super().__init__()
        self.target = target
        self.graph = globalGraph
        self.pool = rafThreadPool
        self.messageQueue = messageQueue
        self.subgraph = set()
        self.finished = False

        with planersLock:
            activePlaners.append(self)

    def send(self, msg):
        if self.messageQueue:
            self.messageQueue.put(msg)
        else:
            print(msg)

    # popunjava se subgraph sa cvororvima koji su potrebni da bi se target izvrsio
    def find_subgraph(self, node_id):
        if node_id not in self.graph.nodes:
            self.send(f"Node '{node_id}' not found in graph.")
            return
        if node_id in self.subgraph:
            return
        self.subgraph.add(node_id)
        node = self.graph.nodes[node_id]
        for dep in node.deps:
            self.find_subgraph(dep)

    # proverama da li su sve deps-ovi zavrseni
    def is_ready(self, node_id):
        node = self.graph.nodes[node_id]
        return all(self.graph.nodes[d].get_state() == "DONE" for d in node.deps)

    # izvrsava action od cvora
    def execute_node(self, node):
        act = node.action
        if not act:
            time.sleep(0.1)
            return f"[OK] Node {node.id} has no action (meta-node)."
        typ = act.get("type")
        cmd = act.get("cmd")

        if not typ or not cmd:
            raise RuntimeError(f"Node {node.id} has invalid action!")

        try:
            if typ == "shell":
                res = sp.run(
                    cmd,
                    shell=True,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                return f"[OK] Node {node.id} (shell) -> {res.stdout.strip()}"
            elif typ == "py":
                exec(cmd, {}, {})
                return f"[OK] Node {node.id} (python) -> Completed"
            else:
                raise ValueError(f"Unknown action type: {typ}")
        except sp.CalledProcessError as e:
            raise RuntimeError(
                f"Command failed for node {node.id}: {e.stderr or str(e)}"
            )
        except Exception as e:
            raise RuntimeError(f"Execution error in node {node.id}: {e}")

    # metoda oznacava da je cvor zavrsen
    def on_done(self, result, node):
        self.graph.release_resources(node.resources)
        node.set_state("DONE")
        self.send(result)
        self.update_dependents(node)
        self.notify_planner()

    # ovde smanjujemo dependecije i ako su svi zavrseni postavlja se cvor na ready
    def update_dependents(self, node):
        for dep_id, dep_node in self.graph.nodes.items():
            if node.id in dep_node.deps:
                dep_node.decrement_deps()
                if self.is_ready(dep_id):
                    dep_node.set_state("READY")
                    self.send(f"Node {dep_id} is now READY!")

    # ako dodje do greske pri izvrsavanju cvora
    def on_fail(self, error, node):
        self.graph.release_resources(node.resources)
        node.set_state("FAILED")
        node.set_error(str(error))
        self.send(f"Node {node.id} failed: {error}")
        self.notify_planner()

    # javlja drugim threadovima da se nesto desilo
    def notify_planner(self):
        with plannerCondition:
            plannerCondition.notify_all()

    # ovde se formira skup svih cvorova koji su potrebni za izvrsavanje targeta
    def initialize_subgraph(self):
        self.send(f"Starting planner for target '{self.target}'...")
        self.find_subgraph(self.target)

        for node_id in self.subgraph:
            node = self.graph.nodes[node_id]
            if self.is_ready(node_id):
                node.set_state("READY")

        self.send(f"Subgraph for '{self.target}': {', '.join(self.subgraph)}")

    # pokrece cvorove ako su ready i ako ima resursa
    def dispatch_ready_nodes(self):
        dispatched = False
        with plannerCondition:
            ready_nodes = [
                node_id
                for node_id in self.subgraph
                if self.graph.nodes[node_id].get_state() == "READY"
            ]
            for node_id in ready_nodes:
                node = self.graph.nodes[node_id]
                if self.graph.acquire_resources(node.resources):
                    node.set_state("RUNNING")
                    self.send(f"Running node: {node_id}")
                    self.pool.apply_async(
                        func=self.execute_node,
                        args=(node,),
                        callback=self.on_done,
                        callback_args=(node,),
                        err_callback=self.on_fail,
                        err_args=(node,),
                    )
                    dispatched = True
        return dispatched

    # proverava da li su svi cvororvi u subgraphu zavrseni
    def check_completion(self):
        all_done = True
        failed = False
        for node_id in self.subgraph:
            state = self.graph.nodes[node_id].get_state()
            if state not in ("DONE", "FAILED"):
                all_done = False
            if state == "FAILED":
                failed = True
        return all_done, failed

    # pokrece sve ready cvorove
    def main_loop(self):
        while not self.finished:
            dispatched = self.dispatch_ready_nodes()
            all_done, failed = self.check_completion()

            if all_done:
                msg = (
                    "Build finished with errors."
                    if failed
                    else "All nodes successfully completed."
                )
                self.send(msg)
                self.finished = True
                break

            if not dispatched:
                with plannerCondition:
                    plannerCondition.wait(timeout=1.0)

    # planer je zavrsio uklanja se iz liste planera
    def cleanup(self):
        with planersLock:
            if self in activePlaners:
                activePlaners.remove(self)

    def run(self):
        try:
            self.initialize_subgraph()
            self.main_loop()
        finally:
            self.cleanup()


class RafThreadPool:
    def __init__(self, num_threads):
        self.num_threads = num_threads
        self.tasks = Queue()
        self.threads = []
        self.active_count = 0
        self.closed = False
        self.lock = Lock()

        for _ in range(num_threads):
            thread = Thread(target=self._worker, daemon=True)
            thread.start()
            self.threads.append(thread)

    # uzima se sledeci zadatak iz reda
    def get_task(self):
        try:
            return self.tasks.get(timeout=0.3)
        except Empty:
            if self.is_closed():
                return None
            return "WAIT"

    def inc_active(self):
        with self.lock:
            self.active_count += 1

    def dec_active(self):
        with self.lock:
            self.active_count -= 1

    # izvrsava zadatke
    def execute_task(self, task):
        func, args, callback, cb_args, err_cb, err_args, future = task
        self.inc_active()
        try:
            res = func(*args)
            future.set_result(res)
            if callback:
                callback(res, *(cb_args or ()))
        except Exception as e:
            future.set_exception(e)
            if err_cb:
                err_cb(e, *(err_args or ()))
        finally:
            self.dec_active()
            self.tasks.task_done()

    # uzima zadatake i prosledjuje
    def _worker(self):
        while True:
            task = self.get_task()
            if task is None:
                break
            if task == "WAIT":
                continue
            self.execute_task(task)

    # stavlja nove zadatke u red
    def apply_async(
        self,
        func,
        args=(),
        callback=None,
        callback_args=None,
        err_callback=None,
        err_args=None,
    ):
        if self.is_closed():
            raise RuntimeError("Thread pool is closed.")
        future = Future()
        self.tasks.put(
            (func, args, callback, callback_args, err_callback, err_args, future)
        )
        return future

    def close(self):
        with self.lock:
            self.closed = True

    def is_closed(self):
        with self.lock:
            return self.closed

    def num_active(self):
        with self.lock:
            return self.active_count

    # ceka da se niti zavrse pa onda salje none
    def join(self):
        self.tasks.join()
        for _ in self.threads:
            self.tasks.put(None)
        for t in self.threads:
            t.join()

    # uklanaj sve zadatke koji nisu pokrenuti
    def cancel_pending_tasks(self):
        cancelled = []
        while not self.tasks.empty():
            try:
                task = self.tasks.get_nowait()
                cancelled.append(task)
            except Empty:
                break
        return cancelled

    # oznacava zadatke kao neuspesne
    def reject_cancelled_tasks(self, cancelled):
        for func, args, cb, cb_args, err_cb, err_args, future in cancelled:
            err = RuntimeError("Thread pool terminated.")
            if err_cb:
                err_cb(err, *(err_args or ()))
            future.set_exception(err)

    # gasi sve niti
    def shutdown_threads(self):
        for _ in self.threads:
            self.tasks.put(None)
        for t in self.threads:
            t.join()

    # gasi ceo pool
    def terminate(self):
        with self.lock:
            self.closed = True

        cancelled = self.cancel_pending_tasks()
        self.reject_cancelled_tasks(cancelled)

        for _ in self.threads:
            self.tasks.put(None)

        for t in self.threads:
            t.join()


class Future:
    def __init__(self):
        self._done = Event()
        self._result = None
        self._exception = None
        self._lock = Lock()

    def set_result(self, res):
        with self._lock:
            self._result = res
            self._done.set()

    def set_exception(self, exc):
        with self._lock:
            self._exception = exc
            self._done.set()

    def result(self, timeout=None):
        if not self._done.wait(timeout):
            raise TimeoutError("Task did not finish in time.")
        with self._lock:
            if self._exception:
                raise self._exception
            return self._result

    def exception(self, timeout=None):
        if not self._done.wait(timeout):
            raise TimeoutError("Task did not finish in time.")
        with self._lock:
            return self._exception

    def done(self):
        return self._done.is_set()


class MyProcessPool:
    def __init__(self, num_processes):
        self.num_processes = num_processes
        self.pool = mp.Pool(processes=num_processes)
        self.lock = Lock()
        self.closed = False
        self.active_count = 0
        self.pending = set()

    def inc_active(self):
        with self.lock:
            self.active_count += 1

    def dec_active(self):
        with self.lock:
            if self.active_count > 0:
                self.active_count -= 1

    # dodavanje zadataka u pool
    def apply_async(
        self,
        func,
        args=(),
        callback=None,
        callback_args=None,
        err_callback=None,
        err_args=None,
    ):
        if self.is_closed():
            raise RuntimeError("Process pool is closed.")

        call_func = func
        call_args = args

        try:
            if getattr(func, "__name__", None) == "execute_node" and args:
                node_obj = args[0]
                payload = {
                    "id": getattr(node_obj, "id", None),
                    "action": getattr(node_obj, "action", None),
                }
                call_func = execute_node_process_safe
                call_args = (payload,)
        except Exception:
            pass

        future = Future()
        self.inc_active()

        result = self.pool.apply_async(call_func, args=call_args)

        def on_success(res, handle=result):
            try:
                future.set_result(res)
                if callback:
                    callback(res, *(callback_args or ()))
            finally:
                with self.lock:
                    self.pending.discard(handle)
                self.dec_active()

        def on_error(err, handle=result):
            try:
                future.set_exception(err)
                if err_callback:
                    err_callback(err, *(err_args or ()))
            finally:
                with self.lock:
                    self.pending.discard(handle)
                self.dec_active()

        result._callback = on_success
        result._error_callback = on_error

        with self.lock:
            self.pending.add(result)

        return future

    def close(self):
        with self.lock:
            self.closed = True

        self.pool.close()

    def is_closed(self):
        with self.lock:
            return self.closed

    def num_active(self):
        with self.lock:
            return self.active_count

    def join(self):
        self.pool.join()

    def terminate(self):
        with self.lock:
            self.closed = True
            pending = list(self.pending)
            self.pending.clear()

        self.pool.terminate()


# treba ova funckija zbog  pickle-safe
def execute_node_process_safe(arg0):
    if isinstance(arg0, dict):
        node_id = arg0.get("id")
        act = arg0.get("action")
    else:

        node_id = getattr(arg0, "id", None)
        act = getattr(arg0, "action", None)

    if not act:
        time.sleep(0.1)
        return f"[OK] Node {node_id} has no action (meta-node)."

    typ, cmd = act.get("type"), act.get("cmd")

    if not typ or not cmd:
        raise RuntimeError(f"Node {node_id} has invalid action!")

    try:
        if typ == "shell":
            res = sp.run(
                cmd,
                shell=True,
                check=True,
                stdout=sp.PIPE,
                stderr=sp.PIPE,
                text=True,
            )
            return f"[OK] Node {node_id} (shell) -> {res.stdout.strip()}"
        elif typ == "py":
            exec(cmd, {}, {})
            return f"[OK] Node {node_id} (python) -> Completed"
        else:
            raise ValueError(f"Unknown action type: {typ}")
    except sp.CalledProcessError as e:
        raise RuntimeError(f"Command failed for node {node_id}: {e.stderr or str(e)}")
    except Exception as e:
        raise RuntimeError(f"Execution error in node {node_id}: {e}")


def load_graph(path: str, messageQueue: Queue):
    global globalGraph

    try:
        with open(path, "r") as file:
            dagData = json.load(file)
    except FileNotFoundError as e:
        message = f"[ERROR] File: {path} not found."
        messageQueue.put(message)
        messageQueue.put(None)

    capacity = dagData.get("capacity", {"CPU": 0, "RAM": 0})
    graph = Graph(capacity=capacity)

    for node in dagData.get("nodes", []):
        node = Node(
            id=node.get("id"),
            deps=node.get("deps", []),
            action=node.get("action", None),
            outputs=node.get("outputs", []),
            resources=node.get("resources", {"CPU": 0, "RAM": 0}),
        )

        if node.resources["CPU"] > capacity["CPU"]:
            messageQueue.put(f"[ERROR] Node {node.id} exceeds available CPU resources.")
            messageQueue.put(None)
            return

        if node.resources["RAM"] > capacity["RAM"]:
            messageQueue.put(f"[ERROR] Node {node.id} exceeds available RAM resources.")
            messageQueue.put(None)
            return

        graph.add_node(node=node)

    for node in graph.nodes.values():
        for dep in node.deps:
            if dep not in graph.nodes:
                messageQueue.put(
                    f"[ERROR] Node '{node.id}' depends on undefined node '{dep}'."
                )
                messageQueue.put(None)
                return

    globalGraph = graph
    messageQueue.put(
        f"[OK] Graph loaded from {path} with {len(globalGraph.nodes)} nodes."
    )
    messageQueue.put(None)


def handle_command(command: str, messageQueue: Queue):
    global globalGraph, rafThreadPool
    try:
        if command.startswith("load "):
            path = command[5:].strip()
            if globalGraph is not None:
                messageQueue.put("Graph already loaded. Reloading is not allowed.")
            else:
                load_graph(path, messageQueue)

        elif command.startswith("build "):
            target = command[6:].strip()
            if globalGraph is None:
                messageQueue.put(
                    "No graph loaded. Please load one using 'load <path>'."
                )
            elif target not in globalGraph.nodes:
                messageQueue.put(f"Unknown target: {target}")
            else:
                planer = Planer(target, globalGraph, rafThreadPool, messageQueue)
                planer.daemon = True
                planer.start()

        elif command == "clean":
            if globalGraph is None:
                messageQueue.put("No graph loaded!")
            elif any(n.get_state() == "RUNNING" for n in globalGraph.nodes.values()):
                messageQueue.put("Cannot clean right now — a build is in progress!")
            else:
                messageQueue.put("CLEAN_CONFIRMATION")
                response_queue = Queue()
                messageQueue.put(response_queue)
                try:
                    confirmation = response_queue.get(timeout=10).strip().lower()
                except:
                    messageQueue.put("Timed out waiting for user confirmation.")
                    return
                if confirmation == "yes":
                    globalGraph.reset_states()
                    messageQueue.put(
                        "Graph reset — all nodes returned to PENDING state."
                    )
                else:
                    messageQueue.put("Clean operation canceled.")

        elif command == "stats":
            if globalGraph is None:
                messageQueue.put("No graph loaded!")
            else:
                messageQueue.put(globalGraph.node_statistics(rafThreadPool))

        elif command.startswith("describe "):
            node_id = command[9:].strip()
            if globalGraph is None:
                messageQueue.put("No graph loaded!")
            else:
                messageQueue.put(globalGraph.describe_node(node_id))

        elif command == "cancel":
            if globalGraph is None:
                messageQueue.put("No graph loaded. No active build!")
            elif rafThreadPool.num_active() == 0:
                messageQueue.put("No builds are currently running.")
            else:
                messageQueue.put("Canceling all pending tasks...")
                rafThreadPool.terminate()
                rafThreadPool.close()
                rafThreadPool.join()
                rafThreadPool = RafThreadPool(4)
                # rafThreadPool = MyProcessPool(4)

        elif command == "exit":
            if rafThreadPool.num_active() > 0:
                messageQueue.put("An active build is running. Performing cancel first.")
                rafThreadPool.terminate()
            rafThreadPool.close()
            rafThreadPool.join()
            messageQueue.put("Closing thread pool and exiting program.")
            messageQueue.put("EXIT")

        else:
            messageQueue.put("Unknown command. Please try again.")

    except Exception as e:
        messageQueue.put(f"Error while processing command: {e}")
    finally:
        messageQueue.put(None)


def main():
    global globalGraph, rafThreadPool

    globalGraph = None
    rafThreadPool = RafThreadPool(4)
    # rafThreadPool = MyProcessPool(4)

    while True:
        try:
            command = input(
                "Enter command (load <path.json> | build <target> | clean | stats | describe <node_id> | cancel | exit): "
            ).strip()

            messageQueue = Queue()

            mainThread = Thread(
                target=handle_command, args=(command, messageQueue), daemon=True
            )
            mainThread.start()

            while True:
                message = messageQueue.get()

                if message is None:
                    break
                elif message == "CLEAN_CONFIRMATION":
                    responseQueue = messageQueue.get()
                    user_input = input("Are you sure? (yes/no): ").strip()
                    responseQueue.put(user_input)
                elif message == "EXIT":
                    return
                else:
                    print(message)

        except KeyboardInterrupt:
            print("User interrupted the program.")
            break
        except Exception as e:
            print(f"Exception occurred: {e}")


if __name__ == "__main__":
    main()
