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

    def get_database_list
      get full_url("db")
    end

    # def create_database_user(database, username, password)
    #   url = full_url("db/#{database}/users")
    #   data = MultiJson.dump({:name => username, :password => password})
    #   post(url, data)
    # end

    # def update_database_user(database, username, options = {})
    #   url = full_url("db/#{database}/users/#{username}")
    #   data = MultiJson.dump(options)
    #   post(url, data)
    # end

    # def delete_database_user(database, username)
    #   delete full_url("db/#{database}/users/#{username}")
    # end

    # def get_database_user_list(database)
    #   get full_url("db/#{database}/users")
    # end

    # def alter_database_privilege(database, username, admin=true)
    #   update_database_user(database, username, :admin => admin)
    # end

    def write_points(queries = [], time_precision: 's')
      json = []
      
      url = full_url("db/#{@database}/series", "time_precision=#{time_precision}")
      
      queries.each do |(name, points)|
        points = Array(points)
        columns = points.reduce(:merge).keys
        payload = {name: name, points: [], columns: columns}

        points.each do |p|
          payload[:points].push( columns.map{|c| p[c]} )
        end
        
        json << payload
      end
      
      post_data = MultiJson.dump(json)
      post(url, post_data, "Content-Type" => "application/json")
    end

    def query(query)
      url = URI.encode full_url("db/#{@database}/series", "chunked=false&q=#{query}")
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
    
    def request(method, uri, options = {})
      cl = HTTP::Client.new(options.merge(socket_class: Celluloid::IO::TCPSocket))
      ret = cl.request(method, uri)
      yield(ret).tap{ cl.send(:finish_response) }
    end
    
    def get(url)
      request(:get, url) do |response|
        handle_return!(response, true)
      end
    end

    def post(url, data, headers = {})
      headers.merge!( "Content-Type" => "application/json" )
      request(:post, url, headers: headers, body: data) do |response|
        handle_return!(response)
      end
    end

    def delete(url)
      request(:delete, url) do |response|
        handle_return!(response)
      end
    end
    
    def handle_return!(response, json = false)
      data = response.to_s
      
      if response.status == 200
        return json ? MultiJson.load(data) : data
      elsif response.status == 401
        raise AuthenticationError.new(data)
      else
        raise Error.new(data)
      end
    end

    def denormalize_series series
      columns = series['columns']
      series['points'].map { |point| Hash[columns.zip(point)]}
    end
  end
end
