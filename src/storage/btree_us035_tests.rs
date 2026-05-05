use super::node::LeafNode;
use super::{BTree, BTreePageStore, CellValue, MemPageStore};

#[test]
fn leaf_cell_value_decodes_only_requested_key() {
    let mut tree = BTree::create(MemPageStore::new()).expect("create btree");
    for key in 0u8..80 {
        let value = vec![key; 128];
        tree.insert(&[key], &value).expect("insert test key");
    }

    let page = tree.find_leaf(&[35]).expect("find target leaf");
    let (image, _) = tree.store.read_leaf(page).expect("read target leaf");
    let value = LeafNode::cell_value(&image, &[35]).expect("decode target cell");

    match value {
        Some(CellValue::Inline(bytes)) => assert_eq!(bytes, vec![35; 128]),
        Some(CellValue::Overflow { .. }) => panic!("expected inline value"),
        None => panic!("expected target key"),
    }
    assert!(LeafNode::cell_value(&image, &[200]).unwrap().is_none());
}
