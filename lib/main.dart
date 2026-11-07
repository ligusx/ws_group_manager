import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:file_picker/file_picker.dart';
import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

void main() => runApp(const MyApp());

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'WS 分组管理器',
      theme: ThemeData(useMaterial3: true, colorSchemeSeed: Colors.blue),
      home: const HomePage(),
    );
  }
}

/// 单条消息
class WsMessage {
  WsMessage({required this.text, required this.time, required this.incoming});

  final String text;
  final DateTime time;
  final bool incoming;

  Map<String, dynamic> toJson() => {
        'text': text,
        'time': time.toIso8601String(),
        'incoming': incoming,
      };

  static WsMessage fromJson(Map<String, dynamic> json) => WsMessage(
        text: json['text'] as String,
        time: DateTime.parse(json['time'] as String),
        incoming: json['incoming'] as bool? ?? true,
      );
}

/// 单个 WebSocket 连接模型
class WsConnection {
  WsConnection({
    required this.id,
    required this.name,
    required this.url,
    this.enabled = true,
  });

  final String id;
  String name;
  final String url;

  /// 用户是否启用该连接（暂停后不自动重连）
  bool enabled;

  WebSocketChannel? _channel;
  StreamSubscription? _sub;
  Timer? _reconnectTimer;
  bool _manualClose = false;

  final List<WsMessage> messages = [];
  final ValueNotifier<bool> connected = ValueNotifier(false);
  final ValueNotifier<int> messageCount = ValueNotifier(0);

  /// 最近消息变化时通知 UI
  void _notifyMessages() {
    messageCount.value = messages.length;
  }

  void connect() {
    if (!enabled) return;
    _manualClose = false;
    _doConnect();
  }

  void pause() {
    enabled = false;
    _manualClose = true;
    _reconnectTimer?.cancel();
    _sub?.cancel();
    _sub = null;
    _channel?.sink.close();
    _channel = null;
    connected.value = false;
  }

  void resume() {
    if (enabled) return;
    enabled = true;
    connect();
  }

  void _doConnect() {
    if (!enabled) return;
    try {
      _channel = WebSocketChannel.connect(Uri.parse(url));
      connected.value = true;
      _sub = _channel!.stream.listen(
        (data) {
          _addMessage(WsMessage(
            text: data.toString(),
            time: DateTime.now(),
            incoming: true,
          ));
        },
        onDone: _onDisconnected,
        onError: (_) => _onDisconnected(),
      );
    } catch (_) {
      _onDisconnected();
    }
  }

  void _addMessage(WsMessage msg) {
    messages.add(msg);
    if (messages.length > 200) messages.removeAt(0);
    _notifyMessages();
    MessageStore.save(id, messages);
  }

  void _onDisconnected() {
    connected.value = false;
    _sub?.cancel();
    _sub = null;
    _channel = null;
    if (!_manualClose && enabled) {
      _reconnectTimer?.cancel();
      _reconnectTimer = Timer(const Duration(seconds: 3), _doConnect);
    }
  }

  void send(String text) {
    _channel?.sink.add(text);
    _addMessage(WsMessage(
      text: text,
      time: DateTime.now(),
      incoming: false,
    ));
  }

  void clearMessages() {
    messages.clear();
    _notifyMessages();
    MessageStore.save(id, messages);
  }

  void dispose() {
    _manualClose = true;
    _reconnectTimer?.cancel();
    _sub?.cancel();
    _channel?.sink.close();
    connected.dispose();
    messageCount.dispose();
  }

  Map<String, dynamic> toJson() => {
        'id': id,
        'name': name,
        'url': url,
        'enabled': enabled,
      };

  static WsConnection fromJson(Map<String, dynamic> json) => WsConnection(
        id: json['id'] as String,
        name: json['name'] as String,
        url: json['url'] as String,
        enabled: json['enabled'] as bool? ?? true,
      );
}

/// 连接配置存储
class ConnectionStore {
  static const _key = 'ws_connections';

  static Future<List<WsConnection>> load() async {
    final prefs = await SharedPreferences.getInstance();
    final raw = prefs.getString(_key);
    if (raw == null || raw.isEmpty) return [];
    try {
      final list = jsonDecode(raw) as List;
      return list
          .map((e) => WsConnection.fromJson(e as Map<String, dynamic>))
          .toList();
    } catch (_) {
      return [];
    }
  }

  static Future<void> save(List<WsConnection> connections) async {
    final prefs = await SharedPreferences.getInstance();
    final raw = jsonEncode(connections.map((e) => e.toJson()).toList());
    await prefs.setString(_key, raw);
  }
}

/// 消息持久化存储（每个连接一个 key）
class MessageStore {
  static String _keyFor(String id) => 'ws_messages_$id';

  static Future<List<WsMessage>> load(String id) async {
    final prefs = await SharedPreferences.getInstance();
    final raw = prefs.getString(_keyFor(id));
    if (raw == null || raw.isEmpty) return [];
    try {
      final list = jsonDecode(raw) as List;
      return list
          .map((e) => WsMessage.fromJson(e as Map<String, dynamic>))
          .toList();
    } catch (_) {
      return [];
    }
  }

  static Future<void> save(String id, List<WsMessage> messages) async {
    final prefs = await SharedPreferences.getInstance();
    final raw = jsonEncode(messages.map((e) => e.toJson()).toList());
    await prefs.setString(_keyFor(id), raw);
  }

  static Future<void> clear(String id) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.remove(_keyFor(id));
  }
}

class HomePage extends StatefulWidget {
  const HomePage({super.key});

  @override
  State<HomePage> createState() => _HomePageState();
}

class _HomePageState extends State<HomePage> {
  final List<WsConnection> _connections = [];
  bool _loading = true;
  int _seq = 0;

  @override
  void initState() {
    super.initState();
    _loadFromStore();
  }

  Future<void> _loadFromStore() async {
    final list = await ConnectionStore.load();
    // 恢复每个连接的历史消息
    for (final conn in list) {
      final msgs = await MessageStore.load(conn.id);
      conn.messages.addAll(msgs);
      conn.messageCount.value = conn.messages.length;
    }
    if (!mounted) return;
    setState(() {
      _connections.addAll(list);
      _loading = false;
    });
    // 仅连接启用状态的连接
    for (final conn in list) {
      if (conn.enabled) conn.connect();
    }
    for (final c in list) {
      final n = int.tryParse(c.id);
      if (n != null && n >= _seq) _seq = n + 1;
    }
  }

  Future<void> _persist() => ConnectionStore.save(_connections);

  Future<void> _addConnection() async {
    final result = await showDialog<Map<String, String>>(
      context: context,
      builder: (_) => const _AddDialog(),
    );
    if (result == null) return;
    final conn = WsConnection(
      id: '${_seq++}',
      name: result['name']!,
      url: result['url']!,
    );
    conn.connect();
    setState(() => _connections.add(conn));
    await _persist();
  }

  Future<void> _rename(WsConnection conn) async {
    final controller = TextEditingController(text: conn.name);
    final newName = await showDialog<String>(
      context: context,
      builder: (_) => AlertDialog(
        title: const Text('重命名'),
        content: TextField(controller: controller, autofocus: true),
        actions: [
          TextButton(
              onPressed: () => Navigator.pop(context),
              child: const Text('取消')),
          FilledButton(
            onPressed: () => Navigator.pop(context, controller.text.trim()),
            child: const Text('保存'),
          ),
        ],
      ),
    );
    if (newName != null && newName.isNotEmpty) {
      setState(() => conn.name = newName);
      await _persist();
    }
  }

  Future<void> _remove(WsConnection conn) async {
    conn.dispose();
    await MessageStore.clear(conn.id);
    setState(() => _connections.remove(conn));
    await _persist();
  }

  Future<void> _toggleEnabled(WsConnection conn, bool value) async {
    setState(() {
      if (value) {
        conn.resume();
      } else {
        conn.pause();
      }
    });
    await _persist();
  }

  Future<void> _clearMessages(WsConnection conn) async {
    conn.clearMessages();
    await MessageStore.clear(conn.id);
  }

  // ---------- 导入 / 导出 ----------

  Future<void> _exportConfig() async {
    final data = {
      'version': 1,
      'connections': _connections.map((e) => e.toJson()).toList(),
    };
    final text = const JsonEncoder.withIndent('  ').convert(data);

    try {
      final path = await FilePicker.platform.saveFile(
        dialogTitle: '保存配置',
        fileName: 'ws_config.json',
        type: FileType.custom,
        allowedExtensions: ['json'],
      );
      if (path != null) {
        final file = File(path);
        await file.writeAsString(text);
        if (mounted) {
          ScaffoldMessenger.of(context).showSnackBar(
            const SnackBar(content: Text('配置已导出')),
          );
        }
      }
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context)
            .showSnackBar(SnackBar(content: Text('导出失败：$e')));
      }
    }
  }

  Future<void> _importConfig() async {
    try {
      final result = await FilePicker.platform.pickFiles(
        type: FileType.custom,
        allowedExtensions: ['json'],
        withData: true,
      );
      if (result == null || result.files.isEmpty) return;
      final bytes = result.files.single.bytes;
      if (bytes == null) return;

      final data = jsonDecode(utf8.decode(bytes)) as Map<String, dynamic>;
      final list = (data['connections'] as List)
          .map((e) => WsConnection.fromJson(e as Map<String, dynamic>))
          .toList();

      // 关闭并移除现有连接
      for (final c in _connections) {
        c.dispose();
      }
      _connections.clear();

      setState(() {
        _connections.addAll(list);
        _seq = 0;
        for (final c in list) {
          final n = int.tryParse(c.id);
          if (n != null && n >= _seq) _seq = n + 1;
        }
      });

      // 恢复消息并连接
      for (final conn in list) {
        final msgs = await MessageStore.load(conn.id);
        conn.messages.addAll(msgs);
        conn.messageCount.value = conn.messages.length;
        if (conn.enabled) conn.connect();
      }

      await _persist();
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('已导入 ${list.length} 个连接')),
        );
      }
    } catch (e) {
      if (mounted) {
        ScaffoldMessenger.of(context)
            .showSnackBar(SnackBar(content: Text('导入失败：$e')));
      }
    }
  }

  @override
  void dispose() {
    for (final c in _connections) {
      c.dispose();
    }
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    if (_loading) {
      return const Scaffold(body: Center(child: CircularProgressIndicator()));
    }
    return Scaffold(
      appBar: AppBar(
        title: const Text('WS 分组管理器'),
        actions: [
          PopupMenuButton<String>(
            onSelected: (v) {
              if (v == 'import') _importConfig();
              if (v == 'export') _exportConfig();
            },
            itemBuilder: (_) => const [
              PopupMenuItem(value: 'import', child: Text('导入配置')),
              PopupMenuItem(value: 'export', child: Text('导出配置')),
            ],
          ),
        ],
      ),
      body: _connections.isEmpty
          ? const Center(child: Text('还没有连接，点右下角添加'))
          : ListView.builder(
              itemCount: _connections.length,
              itemBuilder: (context, index) {
                final conn = _connections[index];
                return _ConnectionTile(
                  conn: conn,
                  onRename: () => _rename(conn),
                  onRemove: () => _remove(conn),
                  onToggleEnabled: (v) => _toggleEnabled(conn, v),
                  onClearMessages: () => _clearMessages(conn),
                );
              },
            ),
      floatingActionButton: FloatingActionButton(
        onPressed: _addConnection,
        child: const Icon(Icons.add),
      ),
    );
  }
}

/// 每个连接一个可展开的分组
class _ConnectionTile extends StatelessWidget {
  const _ConnectionTile({
    required this.conn,
    required this.onRename,
    required this.onRemove,
    required this.onToggleEnabled,
    required this.onClearMessages,
  });

  final WsConnection conn;
  final VoidCallback onRename;
  final VoidCallback onRemove;
  final ValueChanged<bool> onToggleEnabled;
  final VoidCallback onClearMessages;

  @override
  Widget build(BuildContext context) {
    return Card(
      margin: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
      child: ExpansionTile(
        leading: ValueListenableBuilder<bool>(
          valueListenable: conn.connected,
          builder: (_, ok, __) {
            final color = !conn.enabled
                ? Colors.grey
                : (ok ? Colors.green : Colors.red);
            return Icon(Icons.circle, size: 12, color: color);
          },
        ),
        title: Row(
          children: [
            Expanded(
              child: Text(
                conn.name,
                style: TextStyle(
                  fontWeight: FontWeight.bold,
                  color: conn.enabled ? null : Colors.grey,
                ),
                overflow: TextOverflow.ellipsis,
              ),
            ),
            // 启用/暂停开关
            Switch(
              value: conn.enabled,
              onChanged: onToggleEnabled,
            ),
          ],
        ),
        subtitle: Text(conn.url, style: const TextStyle(fontSize: 12)),
        trailing: PopupMenuButton<String>(
          onSelected: (v) {
            if (v == 'rename') onRename();
            if (v == 'clear') onClearMessages();
            if (v == 'remove') onRemove();
          },
          itemBuilder: (_) => const [
            PopupMenuItem(value: 'rename', child: Text('重命名')),
            PopupMenuItem(value: 'clear', child: Text('清空消息')),
            PopupMenuItem(value: 'remove', child: Text('删除')),
          ],
        ),
        children: [
          SizedBox(height: 260, child: _MessageList(conn: conn)),
          _SendBar(conn: conn),
        ],
      ),
    );
  }
}

class _MessageList extends StatelessWidget {
  const _MessageList({required this.conn});
  final WsConnection conn;

  @override
  Widget build(BuildContext context) {
    return ValueListenableBuilder<int>(
      valueListenable: conn.messageCount,
      builder: (context, count, child) {
        final msgs = conn.messages;
        if (msgs.isEmpty) {
          return const Center(
              child: Text('暂无消息', style: TextStyle(color: Colors.grey)));
        }
        return ListView.builder(
          padding: const EdgeInsets.symmetric(horizontal: 12),
          itemCount: msgs.length,
          itemBuilder: (_, i) {
            final m = msgs[i];
            final time = '${m.time.hour.toString().padLeft(2, '0')}:'
                '${m.time.minute.toString().padLeft(2, '0')}:'
                '${m.time.second.toString().padLeft(2, '0')}';
            return Padding(
              padding: const EdgeInsets.symmetric(vertical: 2),
              child: Row(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    m.incoming ? '↓' : '↑',
                    style: TextStyle(
                      color: m.incoming ? Colors.green : Colors.blue,
                      fontWeight: FontWeight.bold,
                    ),
                  ),
                  const SizedBox(width: 4),
                  Text(
                    time,
                    style: const TextStyle(
                      fontSize: 11,
                      color: Colors.grey,
                      fontFamily: 'monospace',
                    ),
                  ),
                  const SizedBox(width: 6),
                  Expanded(
                    child: SelectableText(
                      m.text,
                      style: const TextStyle(
                          fontFamily: 'monospace', fontSize: 12),
                    ),
                  ),
                ],
              ),
            );
          },
        );
      },
    );
  }
}

class _SendBar extends StatefulWidget {
  const _SendBar({required this.conn});
  final WsConnection conn;

  @override
  State<_SendBar> createState() => _SendBarState();
}

class _SendBarState extends State<_SendBar> {
  final TextEditingController _controller = TextEditingController();

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(12, 4, 12, 12),
      child: Row(
        children: [
          Expanded(
            child: TextField(
              controller: _controller,
              decoration: const InputDecoration(
                hintText: '输入要发送的消息',
                isDense: true,
                border: OutlineInputBorder(),
              ),
              onSubmitted: (_) => _send(),
            ),
          ),
          const SizedBox(width: 8),
          FilledButton(onPressed: _send, child: const Text('发送')),
        ],
      ),
    );
  }

  void _send() {
    final text = _controller.text;
    if (text.isEmpty) return;
    if (!widget.conn.enabled) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('连接已暂停，请先启用')),
      );
      return;
    }
    widget.conn.send(text);
    _controller.clear();
  }
}

class _AddDialog extends StatefulWidget {
  const _AddDialog();

  @override
  State<_AddDialog> createState() => _AddDialogState();
}

class _AddDialogState extends State<_AddDialog> {
  final _name = TextEditingController();
  final _url = TextEditingController(text: 'ws://echo.websocket.events');

  @override
  void dispose() {
    _name.dispose();
    _url.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return AlertDialog(
      title: const Text('添加 WebSocket'),
      content: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          TextField(
            controller: _name,
            decoration: const InputDecoration(labelText: '名称'),
          ),
          const SizedBox(height: 12),
          TextField(
            controller: _url,
            decoration: const InputDecoration(labelText: 'WebSocket 地址'),
          ),
        ],
      ),
      actions: [
        TextButton(
            onPressed: () => Navigator.pop(context), child: const Text('取消')),
        FilledButton(
          onPressed: () {
            final name = _name.text.trim();
            final url = _url.text.trim();
            if (name.isEmpty || url.isEmpty) return;
            Navigator.pop(context, {'name': name, 'url': url});
          },
          child: const Text('添加'),
        ),
      ],
    );
  }
}