dofile("table_show.lua")
dofile("urlcode.lua")

local item_type = os.getenv('item_type')
local item_value = os.getenv('item_value')
local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')

local ids = {}

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

local discovered_blogs = {}
local discovered_posts = {}
local discovered_media = {}
local discovered_tags = {}
local posts = {}

local initial = true

if item_type == "blog" then
  discovered_posts[os.time(os.date("!*t"))] = true
end

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

allowed = function(url, parenturl)
  if string.match(url, "'+")
      or string.match(url, "[<>\\%*%$;%^%[%],%(%)]")
      or string.match(url, "^https?://px%.srvcs%.tumblr%.com")
--      or string.match(url, "^https?://[^%.]+%.tumblr%.com/archive/[0-9]+/[0-9]+/?%?before_time=[0-9]+$")
      or string.match(url, "^https?://[^%.]+%.tumblr%.com/reblog/")
      or string.find(url, "/:year")
      or string.find(url, "/:month")
      or string.find(url, "/:id")
      or string.find(url, "/:page")
      or string.find(url, "/:blog_not_found")
      or string.find(url, "/:tag")
      or string.find(url, "/:tumblelog_name")
      or string.find(url, "/:post_id")
      or string.find(url, "/:width")
      or string.find(url, "/:tumblelog_mention_key")
      or string.find(url, "%%2F%%3Ayear")
      or string.find(url, "%%2F%%3Amonth")
      or string.find(url, "%%2F%%3Aid")
      or string.find(url, "%%2F%%3Apage")
      or string.find(url, "%%2F%%3Ablog_not_found")
      or string.find(url, "%%2F%%3Atag")
      or string.find(url, "%%2F%%3Atumblelog_name")
      or string.find(url, "%%2F%%3Apost_id")
      or string.find(url, "%%2F%%3Awidth")
      or string.find(url, "%%2F%%3Atumblelog_mention_key") then
    return false
  end

  if string.match(url, "^https?://[^%.]+%.tumblr%.com") then
    local blogname = string.match(url, "^https?://([^%.]+)%.tumblr%.com")
    if blogname ~= item_value
        and blogname ~= "www"
        and blogname ~= "static"
        and blogname ~= "assets" then
      discovered_blogs[string.match(url, "^https?://([^%.]+)%.tumblr%.com")] = true
    end
  end

  if string.match(url, "^https?://[a-z0-9]+%.media%.tumblr%.com")
      or string.match(url, "^https?://static%.tumblr%.com")
      or string.match(url, "^https?://assets%.tumblr%.com")
      or string.match(url, "^https?://vtt%.tumblr%.com")
      or string.match(url, "^https?://vt%.tumblr%.com")
      or string.match(url, "^https?://v%.tumblr%.com") then
    discovered_media[url] = true
    return false
  end

  if item_type == "blog" then
    if string.match(url, "^https?://[^%.]+%.tumblr%.com/tagged/[^/]+$")
        and string.match(url, "^https?://([^%.]+)") == item_value then
      discovered_tags[string.match(url, "([^/]+)$")] = true
      return false
    end
    if string.match(url, "^https?://[^%.]+%.tumblr%.com/archive/?%?before_time=[0-9]*$") then
      discovered_posts[string.match(url, "([0-9]+)$")] = true
      return true
    end
    if (string.match(url, "^https?://[^%.]+%.tumblr%.com/post/[0-9]+")
        or string.match(url, "^https?://[^%.]+%.tumblr%.com/image/[0-9]+"))
        and not string.match(url, "/photoset_iframe/") then
      return false
    end
  elseif item_type == "posts" then
    if string.match(url, "^https?://[^%.]+%.tumblr%.com/post/[0-9]+")
        or string.match(url, "^https?://[^%.]+%.tumblr%.com/image/[0-9]+") then
      if parenturl ~= nil
          and string.match(parenturl, "^https?://[^%.]+%.tumblr%.com/archive/?%?before_time=[0-9]*$") then
        posts[string.match(url, "^https?://[^/]+/[^/]+/([0-9]*)")] = true
      end
    elseif string.match(url, "^https?://t%.umblr%.com/redirect")
       or string.match(url, "^https?://tmblr%.co/") then
      return true
    end
  elseif item_type == "tags" then
    if string.match(url, "^https?://[^%.]+%.tumblr%.com/tagged/[^/]+") then
      if posts[string.match(url, "^https?://[^/]+/[^/]+/([^/]+)")] == nil then
        return false
      end
    end
  end

  if string.match(url, "^https?://www%.youtube%.com/embed/") then
    return true
  end

  if string.match(url, "^https?://[^%.]+%.tumblr%.com") then
    if string.match(url, "^https?://www%.tumblr%.com/video/")
        or string.match(url, "^https?://[^%.]+%.tumblr%.com/video_file/")
        or string.match(url, "^https?://[^%.]+%.tumblr%.com/video_file/")
        or string.match(url, "^https?://www%.tumblr%.com/embed/clickthrough/")
        or string.match(url, "^https?://embed%.tumblr%.com/embed/") then
      return true
    end
    local blogname = string.match(url, "^https?://([^%.]+)%.tumblr%.com")
    if item_type == "posts" and (blogname == item_value or string.match(url, "^https?://www%.tumblr%.com/oembed/1%.0")) then
      for i in string.gmatch(url, "([0-9]+)") do
        if posts[i] then
          return true
        end
      end
      return false
    elseif item_type == "blog" and blogname == item_value then
      return true
    elseif item_type == "tags" and blogname == item_value
        and string.match(url, "^https?://[^%.]+%.tumblr%.com/tagged/[^/]+") then
      return true
    end
  end
  
  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if string.match(url, "^https?://px%.srvcs%.tumblr%.com")
      or ((item_type == "blog" or item_type == "tags") and string.match(url, "^https?://www%.tumblr%.com/oembed/1%.0")) then
    return false
  end

  if string.match(url, "^https?://[a-z0-9]+%.media%.tumblr%.com")
      or string.match(url, "^https?://static%.tumblr%.com")
      or string.match(url, "^https?://assets%.tumblr%.com")
      or string.match(url, "^https?://vtt%.tumblr%.com")
      or string.match(url, "^https?://vt%.tumblr%.com")
      or string.match(url, "^https?://v%.tumblr%.com") then
    discovered_media[url] = true
    return false
  end
  
  if (downloaded[url] ~= true and addedtolist[url] ~= true)
      and (allowed(url, parent["url"]) or html == 0) then
    addedtolist[url] = true
    return true
  end
  
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  local function check(urla)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.gsub(url, "&amp;", "&")
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
       and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      check(string.match(url, "^(https?:)")..string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(string.match(url, "^(https?:)")..newurl)
    elseif string.match(newurl, "^\\/") then
      check(string.match(url, "^(https?://[^/]+)")..string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(string.match(url, "^(https?://[^/]+)")..newurl)
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(string.match(url, "^(https?://[^%?]+)")..newurl)
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
       or string.match(newurl, "^[/\\]")
       or string.match(newurl, "^[jJ]ava[sS]cript:")

       or string.match(newurl, "^[mM]ail[tT]o:")
       or string.match(newurl, "^vine:")
       or string.match(newurl, "^android%-app:")
       or string.match(newurl, "^ios%-app:")
       or string.match(newurl, "^%${")) then
      check(string.match(url, "^(https?://.+/)")..newurl)
    end
  end

  if item_type == "tags" and string.match(url, "^https?://[^%.]+%.tumblr%.com/tagged/[^/]+$") then
    local tag = string.match(url, "([^/]+)$")
    posts[tag] = true
    if string.find(tag, "-") then
      posts[string.gsub(tag, "%-", "+")] = true
    end
  end

  if (not (string.match(url, "^https?://[0-9a-z]+%.media%.tumblr%.com")
          or string.match(url, "^https?://vtt%.tumblr%.com")
          or string.match(url, "^https?://ve%.media%.tumblr%.com"))
      and allowed(url, nil))
      or initial then
    html = read_file(file)
    initial = false

    if item_type == "tags" and string.match(url, "^https?://[^%.]+%.tumblr%.com/tagged/.+$") then
      local start, tag = string.match(url, "^(https?://[^%.]+%.tumblr%.com/tagged/)(.+)$")
      if string.find(tag, "-") then
        check(start .. string.gsub(tag, "%-", "+"))
      elseif string.find(tag, "+") then
        check(start .. string.gsub(tag, "%+", "-"))
      end
    end

    if string.match(html, '<title>Request denied%.</title>') then
      abortgrab = true
      return urls
    end

    if (string.match(url, "^https?://[^%.]+%.tumblr%.com/post/[0-9]+/[^/%?]+$")
        or string.match(url, "^https?://[^%.]+%.tumblr%.com/post/[0-9]+$"))
        and not (string.match(url, "/embed$") or string.match(url, "/amp$")) then
      check(url ..  "?is_related_post=1")
      check(url ..  "/embed")
      check(url ..  "/amp")
    end

    if string.match(url, "^https?://[^%.]+%.tumblr%.com/post/[0-9]+/[^/]+/embed$") then
      local embed_key = string.match(html, "&quot;embed_key&quot;:&quot;([^&]+)&quot;")
      local post_id = string.match(url, "^https?://[^/]+/post/([0-9]+)")
      local blogname = string.match(url, "^https?://([^%.]+)%.tumblr%.com")
      if embed_key ~= nil then
        check("https://embed.tumblr.com/embed/post/" .. embed_key .. "/" .. post_id)
        check("https://www.tumblr.com/embed/clickthrough/" .. embed_key .. "/" .. post_id .. "/tumblelog?url=https%3A%2F%2F" .. blogname .. ".tumblr.com%2F")
      end
    end

    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
       checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      check(newurl)
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if (status_code >= 300 and status_code <= 399) then
    local newloc = string.match(http_stat["newloc"], "^([^#]+)")
    if string.match(newloc, "^//") then
      newloc = string.match(url["url"], "^(https?:)") .. string.match(newloc, "^//(.+)")
    elseif string.match(newloc, "^/") then
      newloc = string.match(url["url"], "^(https?://[^/]+)") .. newloc
    elseif not string.match(newloc, "^https?://") then
      newloc = string.match(url["url"], "^(https?://.+/)") .. newloc
    end
    if downloaded[newloc] == true or addedtolist[newloc] == true then
      return wget.actions.EXIT
    end
    if string.match(url["url"], "^https?://[^%.]+%.tumblr%.com/video_file/") then
      discovered_media[newloc] = true
      return wget.actions.EXIT
    end
    if string.match(newloc, "https?://www%.tumblr%.com/privacy/consent") then
      io.stdout:write("Privacy consent failed.\n")
      abortgrab = true
    end
    if string.match(newloc, "https?://www%.tumblr%.com/safe%-mode")
        or string.match(newloc, "^https?://www%.tumblr%.com/register/") then
      return wget.actions.EXIT
    end
  end

  if string.match(url["url"], "^https?://t%.umblr%.com/redirect")
      or string.match(url["url"], "^https?://tmblr%.co/") then
    return wget.actions.EXIT
  end
  
  if (status_code >= 200 and status_code <= 399) then
    downloaded[url["url"]] = true
    downloaded[string.gsub(url["url"], "https?://", "http://")] = true
  end

  if abortgrab == true then
    io.stdout:write("ABORTING...\n")
    return wget.actions.ABORT
  end
  
  if status_code >= 500
      or (status_code >= 400 and status_code ~= 404)
      or status_code  == 0 then
    io.stdout:write("Server returned "..http_stat.statcode.." ("..err.."). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 8
    if tries > maxtries then
      io.stdout:write("\nI give up...\n")
      io.stdout:flush()
      tries = 0
      if allowed(url["url"], nil) then
        return wget.actions.ABORT
      else
        return wget.actions.EXIT
      end
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local file = io.open(item_dir..'/'..warc_file_base..'_data.txt', 'w')
  for blog, _ in pairs(discovered_blogs) do
    file:write("blog:" .. blog .. "\n")
  end
  for time, _ in pairs(discovered_posts) do
    file:write("posts:" .. item_value .. ":" .. time .. "\n")
  end
  file:close()
  local file = io.open(item_dir..'/'..warc_file_base..'_media.txt', 'w')
  for url, _ in pairs(discovered_media) do
    file:write(url .. "\n")
  end
  file:close()
  local file = io.open(item_dir..'/'..warc_file_base..'_tags.txt', 'w')
  for tag, _ in pairs(discovered_tags) do
    file:write(item_value .. ":" .. tag .. "\n")
  end
  file:close()
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end
