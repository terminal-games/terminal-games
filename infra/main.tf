terraform {
  required_providers {
    vultr = {
      source  = "vultr/vultr"
      version = "2.29.1"
    }
    hcloud = {
      source = "hetznercloud/hcloud"
    }
    bunnynet = {
      source  = "BunnyWay/bunnynet"
      version = "0.13.0"
    }
  }
}

provider "vultr" {
  api_key = var.vultr_api_key
}

provider "hcloud" {
  token = var.hcloud_token
}

provider "bunnynet" {
  api_key = var.bunnynet_api_key
}

variable "vultr_api_key" {
  type      = string
  sensitive = true
}

variable "hcloud_token" {
  type      = string
  sensitive = true
}

variable "bunnynet_api_key" {
  type      = string
  sensitive = true
}

variable "ssh_public_key" {
  type      = string
  sensitive = true
}

variable "servers" {
  description = "All infrastructure servers"

  type = list(object({
    node_id          = string
    max_active_apps  = optional(number, 100)
    region_code      = optional(string)
    region_name      = optional(string)
    geolocation_lat  = number
    geolocation_long = number

    hetzner = optional(object({
      location    = string
      server_type = string
    }))

    vultr = optional(object({
      region = string
      plan   = string
    }))
  }))

  validation {
    condition = alltrue([
      for s in var.servers :
      (
        (s.hetzner != null && s.vultr == null) ||
        (s.vultr != null && s.hetzner == null)
      )
    ])
    error_message = "Each server must define exactly ONE of: hetzner or vultr."
  }
}

locals {
  server_configs = {
    for s in var.servers :
    s.node_id => merge(s, {
      region_code = coalesce(try(s.region_code, null), upper(substr(s.node_id, 0, 3)))
      region_name = coalesce(try(s.region_name, null), s.node_id)
    })
  }

  hetzner_servers = {
    for k, v in local.server_configs :
    k => v if v.hetzner != null
  }

  vultr_servers = {
    for k, v in local.server_configs :
    k => v if v.vultr != null
  }

  all_servers = merge(
    {
      for k, server in vultr_instance.servers :
      k => {
        main_ip    = server.main_ip
        v6_main_ip = server.v6_main_ip
      }
    },
    {
      for k, server in hcloud_server.servers :
      k => {
        main_ip    = server.ipv4_address
        v6_main_ip = split("/", server.ipv6_address)[0]
      }
    }
  )

  servers = {
    for k, v in local.server_configs :
    k => merge(v, local.all_servers[k])
  }
}

resource "vultr_instance" "servers" {
  for_each = local.vultr_servers

  plan        = each.value.vultr.plan
  region      = each.value.vultr.region
  os_id       = 2625 # debian 13
  label       = "terminal-games-${each.key}"
  tags        = ["terminal-games"]
  hostname    = "terminal-games-${each.key}"
  enable_ipv6 = true
  user_data   = core::file("./cloud-init.yaml")
  ssh_key_ids = [vultr_ssh_key.terminal_games_key.id]
}

resource "hcloud_server" "servers" {
  for_each = local.hetzner_servers

  name        = "terminal-games-${each.key}"
  server_type = each.value.hetzner.server_type
  image       = "debian-13"
  location    = each.value.hetzner.location
  user_data   = core::file("./cloud-init.yaml")
  ssh_keys    = [hcloud_ssh_key.terminal_games_key.id]

  public_net {
    ipv4_enabled = true
    ipv6_enabled = true
  }
}

resource "bunnynet_dns_record" "servers_ipv4_geo" {
  for_each = local.server_configs

  zone               = bunnynet_dns_zone.primary.id
  name               = ""
  type               = "A"
  value              = local.servers[each.key].main_ip
  smart_routing_type = "Geolocation"
  geolocation_lat    = each.value.geolocation_lat
  geolocation_long   = each.value.geolocation_long
}

resource "bunnynet_dns_record" "servers_ipv6_geo" {
  for_each = local.server_configs

  zone               = bunnynet_dns_zone.primary.id
  name               = ""
  type               = "AAAA"
  value              = local.servers[each.key].v6_main_ip
  smart_routing_type = "Geolocation"
  geolocation_lat    = each.value.geolocation_lat
  geolocation_long   = each.value.geolocation_long
}

resource "bunnynet_dns_record" "servers_ipv4" {
  for_each = local.server_configs

  zone  = bunnynet_dns_zone.primary.id
  name  = each.value.node_id
  type  = "A"
  value = local.servers[each.key].main_ip
}

resource "bunnynet_dns_record" "servers_ipv6" {
  for_each = local.server_configs

  zone  = bunnynet_dns_zone.primary.id
  name  = each.value.node_id
  type  = "AAAA"
  value = local.servers[each.key].v6_main_ip
}

resource "local_file" "ansible_inventory" {
  content = templatefile("${path.module}/inventory.tftpl", {
    servers = local.servers
  })
  filename = "${path.module}/inventory.ini"
}

resource "vultr_ssh_key" "terminal_games_key" {
  name    = "terminal-games-key"
  ssh_key = var.ssh_public_key
}

resource "hcloud_ssh_key" "terminal_games_key" {
  name       = "terminal-games-key"
  public_key = var.ssh_public_key
}

resource "bunnynet_dns_zone" "primary" {
  domain = "terminalgames.net"
}
