



using Microsoft.Extensions.Logging;
using System;
/**
* 命名空间: Hikari 
* 类 名： Logger
* CLR版本： 4.0.30319.42000
* 版本 ：v1.0
* Copyright (c) jinyu  
*/

namespace ZmqBindlib
{
    /// <summary>
    /// 功能描述    ：Logger  
    /// 创 建 者    ：jinyu
    /// 创建日期    ：2018/10/26 20:44:15 
    /// 最后修改者  ：jinyu
    /// 最后修改日期：2023/6/26 20:44:15 
    /// </summary>

    public sealed class Logger
    {
        #region [ 单例模式 ]
       

        private static Logger logger= null;

      
        private  ZmqLogger _logger;

       

        public Logger()
        {
          
            var loggerFactory = LoggerFactory.Create(builder => {
                builder .AddConsole();
            }
);
            ILogger logger = loggerFactory.CreateLogger<Logger>();
            _logger = new ZmqLogger(logger);
        }


        /// <summary>  
        /// 得到单例  
        /// </summary>  
        public static Logger Singleton
        {
            get
            {
                if (logger == null)
                {
                   
                   logger = new Logger();
                   
                }
                return logger;
            }
        }

        /// <summary>
        /// 设置日志框架
        /// </summary>
        public ZmqLogger HKLogger { set { _logger = value; } }
        #endregion

      
        #region [ 接口方法 ]

        #region [ Debug ]

        public void Debug(string message)
        {
           
                this.Log(LogLevel.Debug, message);
            
        }

        public void Debug(string message, Exception exception)
        {
            
                this.Log(LogLevel.Debug, message, exception);
            
        }

        public void DebugFormat(string format, params object[] args)
        {
           
                this.Log(LogLevel.Debug, format, args);
            
        }

        public void DebugFormat(string format, Exception exception, params object[] args)
        {
           
                this.Log(LogLevel.Debug, string.Format(format, args), exception);
            
        }

        #endregion

        #region [ Info ]

        public void Info(string message)
        {
           
                this.Log(LogLevel.Info, message);
            
        }

        public void Info(string message, Exception exception)
        {
           
               this.Log(LogLevel.Info, message, exception);
            
        }

        public void InfoFormat(string format, params object[] args)
        {
          
                this.Log(LogLevel.Info, format, args);
            
        }

        public void InfoFormat(string format, Exception exception, params object[] args)
        {
           
                this.Log(LogLevel.Info, string.Format(format, args), exception);
            
        }

        #endregion

        #region  [ Warn ]

        public void Warn(string message)
        {
           
                this.Log(LogLevel.Warn, message);
            
        }

        public void Warn(string message, Exception exception)
        {
                this.Log(LogLevel.Warn, message, exception);
            
        }

        public void WarnFormat(string format, params object[] args)
        {
           
                this.Log(LogLevel.Warn, format, args);
            
        }

        public void WarnFormat(string format, Exception exception, params object[] args)
        {
            
                this.Log(LogLevel.Warn, string.Format(format, args), exception);
            
        }

        #endregion

        #region  [ Error ]

        public void Error(string message)
        {
           
                this.Log(LogLevel.Error, message);
            
        }

        public void Error(string message, Exception exception)
        {
            
                this.Log(LogLevel.Error, message, exception);
            
        }

        public void ErrorFormat(string format, params object[] args)
        {
          
                this.Log(LogLevel.Error, format, args);
            
        }

        public void ErrorFormat(string format, Exception exception, params object[] args)
        {
           
                this.Log(LogLevel.Error, string.Format(format, args), exception);
            
        }
        #endregion

        #region  [ Fatal ]

        public void Fatal(string message)
        {
           
                this.Log(LogLevel.Fatal, message);
            
        }

        public void Fatal(string message, Exception exception)
        {
            
                this.Log(LogLevel.Fatal, message, exception);
            
        }

        public void FatalFormat(string format, params object[] args)
        {
            
                this.Log(LogLevel.Fatal, format, args);
            
        }

        public void FatalFormat(string format, Exception exception, params object[] args)
        {
            
                this.Log(LogLevel.Fatal, string.Format(format, args), exception);
            
        }
        #endregion

        #endregion

        #region [ 内部方法 ]  
       

        /// <summary>  
        /// 输出普通日志  
        /// </summary>  
        /// <param name="level"></param>  
        /// <param name="format"></param>  
        /// <param name="args"></param>  
        private void Log(LogLevel level, string format, params object[] args)
        {
            switch (level)
            {
                case LogLevel.Debug:
                  
                     _logger.DebugFormat(format, args);
                    break;
                case LogLevel.Info:
                    _logger.InfoFormat(format, args);
                    break;
                case LogLevel.Warn:
                    _logger.WarnFormat(format, args);
                    break;
                case LogLevel.Error:
                    _logger.ErrorFormat(format, args);
                    break;
                case LogLevel.Fatal:
                    _logger.FatalFormat(format, args);
                    break;
            }
        }

        /// <summary>  
        /// 格式化输出异常信息  
        /// </summary>  
        /// <param name="level"></param>  
        /// <param name="message"></param>  
        /// <param name="exception"></param>  
        private void Log(LogLevel level, string message, Exception exception)
        {
            switch (level)
            {
                case LogLevel.Debug:
    
                    _logger.Debug(message, exception);
                    break;
                case LogLevel.Info:
                    _logger.Info(message, exception);
                    break;
                case LogLevel.Warn:
                    _logger.Warn(message, exception);
                    break;
                case LogLevel.Error:
                    _logger.Error(message, exception);
                    break;
                case LogLevel.Fatal:
                    _logger.Fatal(message, exception);
                    break;
            }
        }


        #endregion
    }


}
