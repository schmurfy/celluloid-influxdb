require 'http'
require 'multi_json'
require 'celluloid/io'

module Celluloid
  class InfluxDB
    attr_accessor :host, :port, :username, :password, :database
    attr_accessor :queue, :worker

    def initialize(database, host: '127.0.0.1', port: 8086, username: 'root', password: 'root')
      @database = database
      @host = host
      @port = port
      @username = username
      @password = password
    end

    def create_database(name)
      url = full_url("db")
      data = MultiJson.dump({:name => name})
      post(url, data)
    end

    def delete_database(name)
      delete full_url("db/#{name}")
    end

    def get_database_list
      get full_url("db")
    end

    def create_cluster_admin(username, password)
      url = full_url("cluster_admins")
      data = MultiJson.dump({:name => username, :password => password})
      post(url, data)
    end

    def update_cluster_admin(username, password)
      url = full_url("cluster_admins/#{username}")
      data = MultiJson.dump({:password => password})
      post(url, data)
    end

    def delete_cluster_admin(username)
      delete full_url("cluster_admins/#{username}")
    end

    def get_cluster_admin_list
      get full_url("cluster_admins")
    end

    def create_database_user(database, username, password)
      url = full_url("db/#{database}/users")
      data = MultiJson.dump({:name => username, :password => password})
      post(url, data)
    end

    def update_database_user(database, username, options = {})
      url = full_url("db/#{database}/users/#{username}")
      data = MultiJson.dump(options)
      post(url, data)
    end

    def delete_database_user(database, username)
      delete full_url("db/#{database}/users/#{username}")
    end

    def get_database_user_list(database)
      get full_url("db/#{database}/users")
    end

    def alter_database_privilege(database, username, admin=true)
      update_database_user(database, username, :admin => admin)
    end

    def write_point(name, data, async=false)
      data = data.is_a?(Array) ? data : [data]
      columns = data.reduce(:merge).keys.sort {|a,b| a.to_s <=> b.to_s}
      payload = {:name => name, :points => [], :columns => columns}

      data.each do |p|
        point = []
        columns.each { |c| point << p[c] }
        payload[:points].push point
      end

      if async
        @worker = InfluxDB::Worker.new if @worker.nil?
        @worker.queue.push(payload)
      else
        _write([payload])
      end
    end

    def _write(payload)
      url = full_url("db/#{@database}/series")
      data = MultiJson.dump(payload)

      headers = {"Content-Type" => "application/json"}
      post(url, data, headers)
    end

    def query(query)
      url = URI.encode full_url("db/#{@database}/series", "q=#{query}")
      series = get(url)

      if block_given?
        series.each { |s| yield s['name'], denormalize_series(s) }
      else
        series.reduce({}) do |col, s|
          name                  = s['name']
          denormalized_series   = denormalize_series s
          col[name]             = denormalized_series
          col
        end
      end
    end

    private
    def full_url(path, params=nil)
      "".tap do |url|
        url << "http://#{@host}:#{@port}/#{path}?u=#{@username}&p=#{@password}"
        url << "&#{params}" unless params.nil?
      end
    end
    
    Error = Class.new(RuntimeError)
    AuthenticationError = Class.new(Error)
    
    def get(url)
      ret = HTTP.get(url, socket_class: Celluloid::IO::TCPSocket)
      handle_return!(ret.response, true)
    end

    def post(url, data, headers = {})
      headers.merge!( "Content-Type" => "application/json" )
      response = HTTP.post(url, socket_class: Celluloid::IO::TCPSocket, headers: headers, body: data).response
      handle_return!(response)
    end

    def delete(url)
      ret = HTTP.delete(url, socket_class: Celluloid::IO::TCPSocket)
      handle_return!(ret.response)
    end
    
    def handle_return!(response, json = false)
      if response.status == 200
        return json ? MultiJson.load(response.body) : response
      elsif response.status == 401
        raise AuthenticationError.new(response.body)
      else
        raise Error.new(response.body)
      end
    end

    def denormalize_series series
      columns = series['columns']
      series['points'].map { |point| Hash[columns.zip(point)]}
    end
  end
end
