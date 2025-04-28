# src/utils/profiler.py
"""
CSC790 Information Retrieval - Final Project
Goodreads Sentiment Analysis and Information Retrieval System

Module: profiler.py

This module provides tools for measuring and reporting execution time
and tracking diagnostic messages throughout the application. It enables
performance monitoring across different system components.

Authors:
    Matthew D. Branson (branson773@live.missouristate.edu)
    James R. Brown (brown926@live.missouristate.edu)

Missouri State University
Department of Computer Science
May 1, 2025
"""
import time
from io import StringIO

class Profiler:
    """
    Performance profiling utility for timing operations and logging messages.
    
    This class provides methods for measuring execution time of specific
    code blocks, tracking the total runtime, and collecting diagnostic
    messages. It can generate formatted reports in plain text or LaTeX.
    
    Attributes:
        timings (dict): Dictionary mapping task names to execution times
        start_time (float): Start timestamp of the global timer
        paused_time (float): Total time the global timer has been paused
        log_buffer (StringIO): Buffer for storing diagnostic messages
    """
    def __init__(self):
        """Initialize a new Profiler instance with empty timing and logging state."""
        self.timings = {}
        self.start_time = None
        self.paused_time = 0.0
        self.log_buffer = StringIO()

    def timer(self, task_name):
        """
        Create a context manager for timing a code block.
        
        Args:
            task_name (str): Name of the task to be timed
            
        Returns:
            Timer: A context manager that will time the enclosed code block
            
        Example:
            with profiler.timer("Data Processing"):
                process_data()
        """
        return Timer(task_name, self)

    def log_message(self, message):
        """
        Log a diagnostic message to the buffer.
        
        Args:
            message (str): Message to be logged
        """
        self.log_buffer.write(f"{message}\n")

    def start_global_timer(self):
        """
        Start the global execution timer.
        
        This method records the current time as the start point for
        measuring total execution time.
        """
        self.start_time = time.time()

    def pause_global_timer(self):
        """
        Pause the global execution timer.
        
        This method temporarily stops the global timer, preserving
        the elapsed time so far. Can be resumed with resume_global_timer().
        """
        if self.start_time is not None:
            self.paused_time += time.time() - self.start_time
            self.start_time = None

    def resume_global_timer(self):
        """
        Resume the global execution timer after pausing.
        
        This method restarts the global timer from where it was paused,
        maintaining the cumulative execution time.
        """
        if self.start_time is None:
            self.start_time = time.time()
            self.paused_time = 0.0

    def get_global_time(self):
        """
        Get the total elapsed time from the global timer.
        
        Returns:
            float: Total elapsed time in seconds
        """
        if self.start_time is None:
            return self.paused_time
        return time.time() - self.start_time + self.paused_time
   
    def end_global_timer(self):
        """
        Mark the end time of global timer execution.
        
        This method is provided for symmetry with start_global_timer
        and to explicitly indicate the end of timing.
        """
        self._global_end_time = time.time()

    def generate_report(self, doc_count: int = None, vocab_size: int = None, filename: str = None, latex: bool = False) -> str:
        """
        Generate a formatted performance report.
        
        This method creates a report containing logged messages and
        timing information. The report can be formatted as plain text
        or LaTeX, and optionally saved to a file.
        
        Args:
            doc_count (int, optional): Number of documents processed
            vocab_size (int, optional): Size of the vocabulary
            filename (str, optional): Path to save the report file
            latex (bool, optional): Whether to generate LaTeX-friendly output
            
        Returns:
            str: Report content
        """
        report = StringIO()

        if latex:
            # LaTeX formatted report
            report.write("\\section*{Performance Report}\\n\\n")
            report.write("\\subsection*{Message Log}\\n\\begin{verbatim}\n")
            report.write(self.log_buffer.getvalue())
            report.write("\\end{verbatim}\\n\\subsection*{Timing Breakdown}\\n\\begin{itemize}\n")
            
            for task, duration in self.timings.items():
                report.write(f"\\item {task}: {duration:.4f} seconds\n")
            report.write("\\end{itemize}\\n")
            
            tracked_total = sum(self.timings.values())
            report.write(f"\\textbf{{Tracked Operations Total:}} {tracked_total:.4f} seconds\\n\\n")
            
            global_time = self.get_global_time()
            report.write(f"\\textbf{{Global Execution Time:}} {global_time:.4f} seconds\\n")
            
        else:
            # Plain text formatted report
            report.write("=== Message Log ===\n")
            report.write(self.log_buffer.getvalue())
            report.write("\n=== Timing Breakdown ===\n")
            
            for task, duration in self.timings.items():
                report.write(f"{task}: {duration:.4f}s\n")
            
            tracked_total = sum(self.timings.values())
            report.write(f"\nTracked Operations Total: {tracked_total:.4f}s\n")
            
            global_time = self.get_global_time()
            report.write(f"Global Execution Time: {global_time:.4f}s\n")
        
        report_content = report.getvalue()

        # Save to file if filename is provided
        if filename:
            with open(filename, "w", encoding="utf-8") as f:
                f.write(report_content)

        return report_content

class Timer:
    """
    Context manager for timing the execution of a code block.
    
    This class is used internally by the Profiler.timer() method to
    provide a convenient way to time specific code blocks.
    
    Attributes:
        task_name (str): Name of the task being timed
        profiler (Profiler): Reference to the parent Profiler instance
        start (float): Start timestamp when entering the context
    """
    def __init__(self, task_name, profiler):
        """
        Initialize a new Timer context manager.
        
        Args:
            task_name (str): Name of the task to be timed
            profiler (Profiler): Reference to the parent Profiler instance
        """
        self.task_name = task_name
        self.profiler = profiler

    def __enter__(self):
        """
        Enter the context and start timing.
        
        Returns:
            Timer: Self reference for context manager
        """
        self.start = time.time()
        return self

    def __exit__(self, *args):
        """
        Exit the context, stop timing, and record the elapsed time.
        
        Args:
            *args: Exception information (if any) passed by the context manager
        """
        elapsed = time.time() - self.start
        self.profiler.timings[self.task_name] = elapsed