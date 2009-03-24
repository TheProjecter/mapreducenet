using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Text.RegularExpressions;

namespace MapReduceDriver
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("hello");
            MapReduceCore.MapReduceSpec<string, string, string, int> coreSpec = new MapReduceCore.MapReduceSpec<string, string, string, int>();
            coreSpec.RegisterMapper(mapper);
            coreSpec.RegisterReducer(reducer);
            // set up how many cluster nodes you want to run on.
            coreSpec.NumberOfTasks = 2;
            string file1 = " In this example (VariableCLA), the Main function is configured to process a variable number of arguments.  The code is set to add up the values of all of the arguments passed to the function and then, after the last argument is included,  return the result of the addition for display.  ";
            string file2 = "While the details of the classes and method signatures may vary, C# and Java use similar concepts for performing a file I/O operation. Both C# and Java have the concept of a file class and associated file read and write methods. Similar Document Object Models (DOM) exist for handling XML content.";
            string file3 = "This example shows how to open files for reading or writing, how to load and save files using FileStream in C#. ";

            //build inputs
            List<KeyValuePair<string, string>> inputs = new List<KeyValuePair<string, string>>();
            inputs.Add(new KeyValuePair<string, string>("file1", file1));
            inputs.Add(new KeyValuePair<string, string>("file2", file2));
            inputs.Add(new KeyValuePair<string, string>("file3", file3));

            coreSpec.Input = inputs;

            coreSpec.Execute();
            Console.Read();

        }


        public static void mapper(List<KeyValuePair<string, string>> p_input,
                                  List<KeyValuePair<string, int>> p_output)
        {
            foreach (KeyValuePair<string, string> kv in p_input)
            {
                string[] words = Regex.Split(kv.Value, " |,");
                foreach (string word in words)
                {
                    p_output.Add(new KeyValuePair<string, int>(word, 1));
                }
            }
        }

        public static void reducer(List<KeyValuePair<string, List<int>>> p_input,
                                  List<KeyValuePair<string, int>> p_output)
        {
            //Console.WriteLine("reducer in execution");
            //Thread.Sleep(1000);
            foreach (KeyValuePair<string, List<int>> kv in p_input)
            {
                int count = 0;
                foreach (int v in kv.Value)
                {
                    count += v;
                }
                p_output.Add(new KeyValuePair<string,int>(kv.Key, count));
            }


        }

    }
}
