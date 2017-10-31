package com.redislabs.provider.redis

import redis.clients.jedis.{JedisPoolConfig, Jedis, JedisPool}
import redis.clients.jedis.exceptions.JedisConnectionException

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._


object ConnectionPool {
  @transient private lazy val pools: ConcurrentHashMap[RedisEndpoint, JedisPool] =
    new ConcurrentHashMap[RedisEndpoint, JedisPool]()
  def connect(re: RedisEndpoint): Jedis = {
    val pool = pools.getOrElseUpdate(re,
      {
        val poolConfig: JedisPoolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(250)
        poolConfig.setMaxIdle(32)
        poolConfig.setTestOnBorrow(true)
        poolConfig.setTestOnReturn(true)
        poolConfig.setTestWhileIdle(true)
        poolConfig.setMinEvictableIdleTimeMillis(60000)
        poolConfig.setTimeBetweenEvictionRunsMillis(30000)
        poolConfig.setNumTestsPerEvictionRun(-1)
        new JedisPool(poolConfig, re.host, re.port, re.timeout, re.auth, re.dbNum)
      }
    )

    var sleepTime: Int = 4
    var conn: Jedis = null
    while (conn == null) {
      System.err.println("ConnectionPool: connect: "
        + " re.host=" + re.host
        + " re.port=" + re.port
        + " re.auth=" + re.auth
        + " re.dbNum=" + re.dbNum
        + " re.timeout="+re.timeout
        + " NumActive=" + pool.getNumActive()
        + " NumIdle=" + pool.getNumIdle()
        + " NumWaiters=" + pool.getNumWaiters()
      )

      try {
        conn = pool.getResource
      }
      catch {
//        case e: JedisConnectionException if e.getCause.toString.
//          contains("ERR max number of clients reached") => {
//          if (sleepTime < 500) sleepTime *= 2
//          Thread.sleep(sleepTime)
//        }
        case e: JedisConnectionException => {
          System.err.println(e.getCause);

          if (sleepTime < 500)
            sleepTime *= 2
          Thread.sleep(sleepTime)
        }
        case e: Exception => throw e
      }
    }
    conn
  }
}

