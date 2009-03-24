using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using System.Threading;


namespace MapReduceCore
{

    public class MapReduceSpec<TKey, TValue, TIKey, TIValue>
    {
        public void testWrite()
        {
            Console.WriteLine("mapreduce core lib");
        }

        // TIKey: intermediate key, TIValue: intermediate value
        // TKey: Input Key, TValue: Input value
        // (TKey,TValue)   (TIkey TIValue) may not be in same domain.
        public delegate void MapDelegate(List< KeyValuePair<TKey, TValue> > p_input,List<KeyValuePair<TIKey, TIValue>>p_output );
        public delegate void ReduceDelegate(List<KeyValuePair<TIKey, List<TIValue>>> p_input,List<KeyValuePair<TIKey, TIValue>> p_output );
        private int m_numberOfTasks=2;

        private List<KeyValuePair<TKey,TValue>> m_inputs;

        public MapDelegate Mapper;
        public ReduceDelegate Reducer;
        public bool RegisterMapper(MapDelegate p_mapper)
        {
            if (p_mapper != null)
            {
                this.Mapper = p_mapper;
                return true;
            }
            else
            {
                return false;
            }
            
            
        }

        public bool RegisterReducer(ReduceDelegate p_reducer)
        {
            this.Reducer = p_reducer;
            return true;
        }

        public int NumberOfTasks
        {
            get
            {
                return m_numberOfTasks;
            }
            set 
            {
                m_numberOfTasks = value;
            }
        }

        public List<KeyValuePair<TKey, TValue>> Input
        {
            get
            {
                return m_inputs;
            }
            set
            {
                m_inputs = value;
            }
        }

        private void InitializeEventArray(ManualResetEvent[] p_eventArray, bool p_bool)
        {
            for (int i = 0; i < p_eventArray.Length; i++)
            {
                p_eventArray[i] = new ManualResetEvent(p_bool);
            }
        }

        private void QueueMapperItem(ManualResetEvent[] p_eventArray, List<KeyValuePair<TKey,TValue>>[]p_mapperInputs, List<KeyValuePair<TIKey, TIValue>>[] p_interMediateoutput)
        {
            for (int i = 0; i < p_eventArray.Length; i++)
            {
                MapperStateArgument<TKey, TValue, TIKey, TIValue> stateArg = new
                                        MapperStateArgument<TKey, TValue, TIKey, TIValue>("Mapper Test arg", p_eventArray[i], p_mapperInputs[i],p_interMediateoutput[i]);
                ThreadPool.QueueUserWorkItem(new WaitCallback(executeMapper), stateArg);
            }
        }

        private void QueueReducerItem(ManualResetEvent[] p_reducerEventArray,List<KeyValuePair<TIKey, List<TIValue>>>[] p_reducerInputs, List<KeyValuePair<TIKey, TIValue>>[] p_reducerOutputs )
        {
            for (int i = 0; i < p_reducerEventArray.Length; i++)
            {
                ReducerStateArgument<TIKey, TIValue> arg = new ReducerStateArgument<TIKey, TIValue>(
                                                            "Reducer arg",
                                                            p_reducerEventArray[i], p_reducerInputs[i], p_reducerOutputs[i]);

                ThreadPool.QueueUserWorkItem(new WaitCallback(executeReducer), arg);
            }
        }

        public void Execute()
        {

            Console.WriteLine(" start: map reduce in execution");
            System.Collections.Generic.Dictionary<TIKey, List<TIValue>> groupIngResult = new Dictionary<TIKey,List<TIValue>>();
            
            //set up manual reset events for mapping  
            ManualResetEvent[] mapFinishedEvents = new ManualResetEvent[m_numberOfTasks];
            InitializeEventArray(mapFinishedEvents, false);
            //setup mapper inputs
            List<KeyValuePair<TKey, TValue>>[] mapperInputs = new List<KeyValuePair<TKey, TValue>>[NumberOfTasks];
            //List<KeyValuePair<TKey, TValue>>[] mapperInputs = Enumerable.Repeat(new List<KeyValuePair<TKey, TValue>>(), NumberOfTasks).ToArray();
            for (int i = 0; i < NumberOfTasks; i++) mapperInputs[i] = new List<KeyValuePair<TKey, TValue>>();
            //split inputs , we can use a better splitter in the future 
            int index = 0 ;
            int counts = m_inputs.Count / NumberOfTasks;

            for (int i = 0; i < NumberOfTasks-1; i++)
            {
                mapperInputs[i].AddRange(m_inputs.GetRange(index,  counts));
                index = index + counts; 
            }
            mapperInputs[NumberOfTasks-1].AddRange(m_inputs.GetRange(index, m_inputs.Count - index));

            // set up mapper intermeidate results
            List<KeyValuePair<TIKey, TIValue>>[] MapperInterMediateOutputs = new List<KeyValuePair<TIKey, TIValue>>[m_numberOfTasks];
            for (int i = 0; i < NumberOfTasks; i++) MapperInterMediateOutputs[i] = new List<KeyValuePair<TIKey, TIValue>>();

            // queue all mapper tasks with those parameters
            QueueMapperItem(mapFinishedEvents,mapperInputs, MapperInterMediateOutputs);
            
            //wait on all mapper to run to finish
            WaitHandle.WaitAll(mapFinishedEvents);

            // this is called barier stage, when all mapping results are ready 
            Dictionary<TIKey, List<TIValue>> temp = new Dictionary<TIKey, List<TIValue>>(); 
           
            // set up a group combiner to combine intermeidate results
            foreach (List<KeyValuePair<TIKey, TIValue>> individualIntermediateResult in MapperInterMediateOutputs)
            {
                // combine the results
                foreach (KeyValuePair<TIKey, TIValue> kv in individualIntermediateResult)
                {
                    if (temp.ContainsKey(kv.Key))
                    {
                        temp[kv.Key].Add(kv.Value);
                    }
                    else
                    {
                        temp.Add(kv.Key, new List<TIValue>());
                        temp[kv.Key].Add(kv.Value);
                    }
                }

            }
            List<KeyValuePair<TIKey, List<TIValue>>> shuffledMapperOutputs = temp.ToList();
            //sort by TIKey 
            shuffledMapperOutputs.Sort(delegate(KeyValuePair<TIKey, List<TIValue>> a, KeyValuePair<TIKey, List<TIValue>> b)
            {
                return a.Key.ToString().CompareTo(b.Key.ToString());
            });
            
            // split into differet input sets for reducer to execute
            List<KeyValuePair<TIKey, List<TIValue>>>[] reducerInputs = new List<KeyValuePair<TIKey, List<TIValue>>>[NumberOfTasks];
            for (int i = 0; i < NumberOfTasks; i++) reducerInputs[i] = new List<KeyValuePair<TIKey, List<TIValue>>>();

            //split Reducer inputs , again, we can use a better splitter 
            index = 0;
            counts = shuffledMapperOutputs.Count/ NumberOfTasks;
            for (int i = 0; i < NumberOfTasks-1; i++)
            {
                reducerInputs[i].AddRange(shuffledMapperOutputs.GetRange(index,counts));
                index = index + counts;
            }
            reducerInputs[NumberOfTasks-1].AddRange(shuffledMapperOutputs.GetRange(index, shuffledMapperOutputs.Count - index));

            //setup reducer finishing events
            ManualResetEvent[] reduceFinishedEvents = new ManualResetEvent[m_numberOfTasks];
            InitializeEventArray(reduceFinishedEvents, false);
            
            //setup reducer outputs
            List<KeyValuePair<TIKey, TIValue>>[] reducerOutputs = new List<KeyValuePair<TIKey,TIValue>>[NumberOfTasks];
            for (int i = 0; i < NumberOfTasks; i++) reducerOutputs[i] = new List<KeyValuePair<TIKey, TIValue>>();


            //queuing reducer
            QueueReducerItem(reduceFinishedEvents, reducerInputs, reducerOutputs);
            // wait all to finish
            WaitHandle.WaitAll(reduceFinishedEvents);
            Console.WriteLine("mapper and reducers are all done");
            foreach (List<KeyValuePair<TIKey, TIValue>> output in reducerOutputs)
            {
                foreach (KeyValuePair<TIKey, TIValue> kv in output)
                {
                    Console.WriteLine(String.Format("[{0}], {1}", kv.Key, kv.Value));
                }
            }
        }

        private void executeMapper(object p_stateArg)
        {
            MapperStateArgument<TKey, TValue, TIKey, TIValue> stateArg = p_stateArg as MapperStateArgument<TKey, TValue, TIKey, TIValue>;
          //  Console.WriteLine("Mapping starts at thread-[{0}], thread sleep to simulate mapping operation", Thread.CurrentThread.GetHashCode());
            DateTime startTime = DateTime.Now;
            // get the result from mapper and put into 
            Mapper(stateArg.MapperInput, stateArg.InterMediateOutput);
            DateTime stopTime = DateTime.Now;
            TimeSpan duration = stopTime - startTime;
            Console.WriteLine("Mapper runs at node-[{0}]. Takes [{1}] to finsih."
                              , Thread.CurrentThread.GetHashCode(), duration);
            stateArg.MapperDoneEvent.Set();

        }

        private void executeReducer(object p_accumlator)
        {
            ReducerStateArgument<TIKey,TIValue> stateArg = p_accumlator as ReducerStateArgument<TIKey,TIValue>;
            DateTime startTime = DateTime.Now;
            Reducer(stateArg.ReducerInput, stateArg.ReducerOutput);
            DateTime stopTime = DateTime.Now;
            TimeSpan duration = stopTime - startTime;
            Console.WriteLine("Reducer runs at node-[{0}]. Takes [{1}] to finsih."
                  , Thread.CurrentThread.GetHashCode(), duration);
            stateArg.ReducerDoneEvent.Set();
            
        }

        private void executeGrouper(object p_stateArg)
        {

        }

    }

    public class MapperStateArgument<TKey, TValue, TIKey, TIValue>
    {
        public MapperStateArgument(string p_str, ManualResetEvent p_MapperdoneEvent, List<KeyValuePair<TKey, TValue>> p_mapperInput, List<KeyValuePair<TIKey, TIValue>> p_intermediate_output)
        {
            Name =p_str;
            MapperDoneEvent = p_MapperdoneEvent;
            MapperInput = p_mapperInput;
            InterMediateOutput = p_intermediate_output;
        }
        public string Name { get; set; }
        public ManualResetEvent MapperDoneEvent { get; set; }
        public List<KeyValuePair<TIKey, TIValue>> InterMediateOutput { get; set; }
        public List<KeyValuePair<TKey, TValue>> MapperInput { get; set; }
    }

    public class ReducerStateArgument<TIKey, TIValue>
    {
        public ReducerStateArgument(string p_str, ManualResetEvent p_MapperdoneEvent, 
                                    List<KeyValuePair<TIKey, List<TIValue>>> p_reducerInput, 
                                    List<KeyValuePair<TIKey, TIValue>> p_reducerOutput)
        {
            Name = p_str;
            ReducerDoneEvent = p_MapperdoneEvent;
            ReducerInput = p_reducerInput;
            ReducerOutput = p_reducerOutput;
        }
        public string Name { get; set; }
        public ManualResetEvent ReducerDoneEvent { get; set; }
        public List<KeyValuePair<TIKey, List<TIValue>>> ReducerInput { get; set; }
        public List<KeyValuePair<TIKey, TIValue>> ReducerOutput { get; set; }

    }
}
