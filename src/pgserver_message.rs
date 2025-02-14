use bytes::BytesMut;

pub enum PGServerMessage {
    AuthenticationRequest,
    BackendKeyData(i32, i32),
    ReadyForQuery,
    RowDescription(Vec<RowDescriptionField>),
    DataRow(Vec<BytesMut>),
    CommandComplete(String),
}

pub struct RowDescriptionField {
    pub name: String,
    pub table_object_id: i32,
    pub column_attribute_number: i16,
    pub data_type_object_id: i32,
    pub data_type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16,
}

impl RowDescriptionField {
    pub fn length(&self) -> usize {
        18 + self.name.len() + 1
    }
}

impl PGServerMessage {
    pub fn encode(message: PGServerMessage) -> BytesMut {
        match message {
            PGServerMessage::AuthenticationRequest => {
                let mut buf = BytesMut::with_capacity(9);
                buf.extend_from_slice(&[b'R']);
                buf.extend_from_slice(&8i32.to_be_bytes());
                buf.extend_from_slice(&0i32.to_be_bytes());
                buf
            },
            PGServerMessage::BackendKeyData(pid, secret) => {
                let mut buf = BytesMut::with_capacity(13);
                buf.extend_from_slice(&[b'K']);
                buf.extend_from_slice(&12i32.to_be_bytes());
                buf.extend_from_slice(&pid.to_be_bytes());
                buf.extend_from_slice(&secret.to_be_bytes());
                buf
            },
            PGServerMessage::ReadyForQuery => {
                let mut buf = BytesMut::with_capacity(6);
                buf.extend_from_slice(&[b'Z']);
                buf.extend_from_slice(&5i32.to_be_bytes());
                buf.extend_from_slice(&[b'I']);
                buf
            },
            PGServerMessage::RowDescription(fields) => {
                let total_field_len: usize = fields.iter().map(|f| f.length()).sum();
                let total_len = 4 + 2 + total_field_len;
                let mut buf = BytesMut::with_capacity(total_len + 1);
                buf.extend_from_slice(&[b'T']);
                buf.extend_from_slice(&(total_len as i32).to_be_bytes());
                buf.extend_from_slice(&(fields.len() as i16).to_be_bytes());
                for field in fields {
                    buf.extend_from_slice(field.name.as_bytes());
                    buf.extend_from_slice(&[0]);
                    buf.extend_from_slice(&field.table_object_id.to_be_bytes());
                    buf.extend_from_slice(&field.column_attribute_number.to_be_bytes());
                    buf.extend_from_slice(&field.data_type_object_id.to_be_bytes());
                    buf.extend_from_slice(&field.data_type_size.to_be_bytes());
                    buf.extend_from_slice(&field.type_modifier.to_be_bytes());
                    buf.extend_from_slice(&field.format_code.to_be_bytes());
                }
                buf
            },
            PGServerMessage::DataRow(values) => {
                let total_len: usize = 4 + 2 + values.iter().map(|v| 4 + v.len()).sum::<usize>();
                let mut buf = BytesMut::with_capacity(total_len + 1);
                buf.extend_from_slice(&[b'D']);
                buf.extend_from_slice(&(total_len as i32).to_be_bytes());
                buf.extend_from_slice(&(values.len() as i16).to_be_bytes());
                for v in values {
                    buf.extend_from_slice(&(v.len() as i32).to_be_bytes());
                    buf.extend_from_slice(&v);
                }
                buf
            },
            PGServerMessage::CommandComplete(cmd) => {
                let total_len = 4 + cmd.len() + 1;
                let mut buf = BytesMut::with_capacity(total_len + 1);
                buf.extend_from_slice(&[b'C']);
                buf.extend_from_slice(&(total_len as i32).to_be_bytes());
                buf.extend_from_slice(cmd.as_bytes());
                buf.extend_from_slice(&[0]);
                buf
            },
        }
    }
}
