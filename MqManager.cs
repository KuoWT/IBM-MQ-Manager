using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Web;
using fst.AVMUtility;
using IBM.WMQ;
using System.Web.Services;

namespace ReplyService
{
    public class MqManager
    {
        
        string queueName;
        MQQueueManager qMgr;
        MQMessage mqMsg;
        MQQueue queue;
        MQPutMessageOptions putOptions;

        #region   MQ Connect

        string linkStatus;
        public bool LinkToQueueManager(string path)
        {
            CIni ini = new CIni(path);
            string QueueName = ini.ReadValue("MQ", "MQ_QUEUE");
            queueName = QueueName;
            string MQCCSID = ini.ReadValue("MQ", "MQ_CCSID");
            Environment.SetEnvironmentVariable("MQCCSID", MQCCSID);
            if (MQEnvironment.properties.Count <= 0)
            {
                MQEnvironment.properties.Add(MQC.CCSID_PROPERTY, MQCCSID);
            }

            MQEnvironment.Port = Convert.ToInt32(ini.ReadValue("MQ", "MQ_PORT"));
            MQEnvironment.Channel = ini.ReadValue("MQ", "MQ_CHANNEL");
            MQEnvironment.Hostname = ini.ReadValue("MQ", "MQ_HOST"); 
            string qmName = ini.ReadValue("MQ", "MQ_QUEUE_MANAGER"); 

            try
            {
                if (qMgr == null || !qMgr.IsConnected)
                {
                    qMgr = new MQQueueManager(qmName);
                }

                linkStatus = "Connect:Successed！";
            }
            catch (MQException e)
            {
               
                linkStatus = "Connect error: code：" + e.CompletionCode + " reson code：" + e.ReasonCode;

                ProgramLog.WriteLineLog(linkStatus);
                return false;
            }
            catch (Exception e)
            {

                linkStatus = "Connect error: code：" + e;
                ProgramLog.WriteLineLog(linkStatus);
                
                return false;
            }
            return qMgr.IsConnected;
        }
        #endregion

        #region   SEND

        public bool SendMsg(string message,string encode)
        {
            int openOptions = MQC.MQOO_OUTPUT | MQC.MQOO_FAIL_IF_QUIESCING;
            try
            {
                queue = qMgr.AccessQueue(queueName, openOptions);   
            }
            catch (MQException e)
            {
                ProgramLog.WriteLineLog(e.Message);
                return false;
            }
            mqMsg = new MQMessage();
            //mqMsg.Encoding = Convert.ToInt32(encode);
            mqMsg.CharacterSet = Convert.ToInt32(encode);
            mqMsg.WriteString(message);
            putOptions = new MQPutMessageOptions();
            try
            {
                queue.Put(mqMsg, putOptions);
                return true;
            }
            catch (MQException mqe)
            {
                return false;
            }
            finally
            {
                try
                {
                    qMgr.Disconnect();
                }
                catch (MQException e)
                {

                }
            }
        }


        #endregion

        #region   接收消息

        public string receiveMsg()
        {
            int openOptions = MQC.MQOO_OUTPUT | MQC.MQOO_INPUT_SHARED | MQC.MQOO_INQUIRE;
            try
            {
                queue = qMgr.AccessQueue(queueName, openOptions);   
            }
            catch (MQException e)
            {
                ProgramLog.WriteLineLog("receiveMsg: "+e.Message);
            }

            MQGetMessageOptions mqGetMsgOpts;
            mqMsg = new MQMessage();
            mqGetMsgOpts = new MQGetMessageOptions();
            mqGetMsgOpts.WaitInterval = 15000;
            mqGetMsgOpts.Options |= MQC.MQGMO_WAIT;
            try
            {
                int queryDep = queue.CurrentDepth;
                if (queryDep > 0)
                {
                    queue.Get(mqMsg, mqGetMsgOpts);         
                    //var ds = new DataSet();
                    //var table = new DataTable("T_School");
                    //table.Columns.Add("ID", typeof(string));
                    //table.Columns.Add("SchoolName", typeof(string));
                    //table.Columns.Add("BuildDate", typeof(string));
                    //table.Columns.Add("Address", typeof(string));
                    //ds.Tables.Add(table);
                    string message = mqMsg.ReadString(mqMsg.MessageLength);
                    mqMsg.Format = MQC.MQFMT_XMIT_Q_HEADER;
                    //var reader = new StringReader(message);
                    //ds.ReadXml(reader, XmlReadMode.Fragment);
                    return message;
                }

                else
                {
                    return null;
                }
            }
            catch (MQException ex)
            {
                ProgramLog.WriteLineLog("receiveMsg: " + ex.InnerException);
               
                return null;
            }
            finally
            {
                try
                {
                    qMgr.Disconnect();

                }
                catch (MQException e)
                {

                }
            }
        }
        #endregion

    }
}