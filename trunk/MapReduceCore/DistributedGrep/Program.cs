using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace DistributedGrep
{
    class Program
    {

        static void Main(string[] args)
        {
            Console.WriteLine("hello");

            string pattern = args[0];
            string path = args[1];


            MapReduceCore.MapReduceSpec<string, string, string, int> coreSpec = new MapReduceCore.MapReduceSpec<string, string, string, int>();
            coreSpec.RegisterMapper(mapper);
            coreSpec.RegisterReducer(reducer);
            coreSpec.NumberOfTasks = 3;

            //build inputs
            List<KeyValuePair<string, string>> inputs = new List<KeyValuePair<string, string>>();
            inputs.Add(new KeyValuePair<string, string>("file1", "test ok geek the p the I am P"));
            inputs.Add(new KeyValuePair<string, string>("file2", "test ok geek"));
            inputs.Add(new KeyValuePair<string, string>("file3", "geek"));

            coreSpec.Input = inputs;

            coreSpec.Execute();
            Console.Read();

        }


        public static void mapper(List<KeyValuePair<string, string>> p_input,
                                  List<KeyValuePair<string, int>> p_output)
        {
            foreach (KeyValuePair<string, string> kv in p_input)
            {
                string[] words = Regex.Split(kv.Value, " ");
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
